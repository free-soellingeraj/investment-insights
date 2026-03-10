"""API endpoints for human ratings/feedback on any entity.

POST /api/ratings           — submit a rating
GET  /api/ratings           — get ratings for an entity (?entity_type=...&entity_id=...)
GET  /api/ratings/recent    — latest 50 ratings (for agent consumption)
GET  /api/ratings/summary   — aggregate stats
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path

from litestar import Request, get, post
from sqlalchemy import text

from ai_opportunity_index.storage.db import (
    create_human_rating,
    get_ratings_for_entity,
    get_ratings_summary,
    get_recent_ratings,
    get_session,
)

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
AGENT_COMMS_DIR = PROJECT_ROOT / ".agent-comms"
HUMAN_FEEDBACK_PATH = AGENT_COMMS_DIR / "human_feedback.md"

# Map action codes to display labels
ACTION_LABELS = {
    "flag_for_review": "FLAG",
    "mark_incorrect": "INCORRECT",
    "approve": "APPROVE",
    "needs_more_evidence": "NEEDS_EVIDENCE",
}


@post("/api/ratings")
async def api_ratings_create(request: Request) -> dict:
    """Submit a human rating/feedback on any entity."""
    try:
        body = await request.json()
    except Exception:
        return {"ok": False, "error": "Invalid JSON body"}

    entity_type = body.get("entity_type")
    entity_id = body.get("entity_id")

    if not entity_type or entity_id is None:
        return {"ok": False, "error": "entity_type and entity_id are required"}

    try:
        entity_id = int(entity_id)
    except (TypeError, ValueError):
        return {"ok": False, "error": "entity_id must be an integer"}

    try:
        result = create_human_rating(
            entity_type=entity_type,
            entity_id=entity_id,
            rating=body.get("rating"),
            dimension=body.get("dimension", "overall"),
            comment=body.get("comment"),
            action=body.get("action"),
            metadata=body.get("metadata"),
        )
    except ValueError as e:
        return {"ok": False, "error": str(e)}
    except Exception as e:
        logger.error("Error creating rating: %s", e)
        return {"ok": False, "error": "Internal error creating rating"}

    # Immediate action: suppress bad projects/valuations on mark_incorrect
    suppressed = False
    action_taken = body.get("action")
    if action_taken == "mark_incorrect" and entity_type in ("project", "valuation"):
        try:
            suppressed = _suppress_entity(entity_type, entity_id, body.get("comment"))
        except Exception as e:
            logger.warning("Failed to suppress %s %d: %s", entity_type, entity_id, e)

    # Update agent-comms feedback file after successful write
    try:
        _update_agent_feedback_file()
    except Exception as e:
        logger.warning("Failed to update agent feedback file: %s", e)

    return {"ok": True, "rating": result, "suppressed": suppressed}


@get("/api/ratings")
async def api_ratings_list(request: Request) -> dict:
    """Get ratings for a specific entity, or recent ratings if no filter."""
    entity_type = request.query_params.get("entity_type")
    entity_id_str = request.query_params.get("entity_id")

    if entity_type and entity_id_str:
        try:
            entity_id = int(entity_id_str)
        except (TypeError, ValueError):
            return {"ok": False, "error": "entity_id must be an integer"}
        ratings = get_ratings_for_entity(entity_type, entity_id)
        return {"ok": True, "ratings": ratings, "count": len(ratings)}
    else:
        limit = int(request.query_params.get("limit", "50"))
        ratings = get_recent_ratings(limit=min(limit, 200))
        return {"ok": True, "ratings": ratings, "count": len(ratings)}


@get("/api/ratings/recent")
async def api_ratings_recent(request: Request) -> dict:
    """Latest ratings — designed for agent consumption."""
    limit = int(request.query_params.get("limit", "50"))
    ratings = get_recent_ratings(limit=min(limit, 200))
    return {"ok": True, "ratings": ratings, "count": len(ratings)}


@get("/api/ratings/summary")
async def api_ratings_summary() -> dict:
    """Aggregate rating statistics."""
    try:
        summary = get_ratings_summary()
        return {"ok": True, **summary}
    except Exception as e:
        logger.error("Error fetching rating summary: %s", e)
        return {"ok": False, "error": str(e)}


def _suppress_entity(entity_type: str, entity_id: int, comment: str | None = None) -> bool:
    """Immediately suppress a project or valuation marked incorrect by a human.

    Sets dollars to 0 and confidence to 0 so the bad data doesn't pollute scores.
    Returns True if a row was updated.
    """
    reason = f"Human marked incorrect: {comment}" if comment else "Human marked incorrect"
    s = get_session()
    try:
        if entity_type == "project":
            result = s.execute(
                text("""
                    UPDATE investment_projects
                    SET dollar_total = 0, dollar_low = 0, dollar_high = 0,
                        confidence = 0, status = 'suppressed',
                        description = description || E'\n[SUPPRESSED: ' || :reason || ']'
                    WHERE id = :id AND dollar_total != 0
                """),
                {"id": entity_id, "reason": reason},
            )
            s.commit()
            updated = result.rowcount > 0
            if updated:
                logger.info("Suppressed project %d: %s", entity_id, reason)
            return updated

        elif entity_type == "valuation":
            result = s.execute(
                text("""
                    UPDATE valuations
                    SET dollar_low = 0, dollar_high = 0, confidence = 0,
                        adjustment_reason = :reason
                    WHERE id = :id AND (dollar_low != 0 OR dollar_high != 0)
                """),
                {"id": entity_id, "reason": reason},
            )
            s.commit()
            updated = result.rowcount > 0
            if updated:
                logger.info("Suppressed valuation %d: %s", entity_id, reason)
            return updated

        return False
    except Exception:
        s.rollback()
        raise
    finally:
        s.close()


def _update_agent_feedback_file():
    """Write recent feedback to .agent-comms/human_feedback.md for agents to read."""
    AGENT_COMMS_DIR.mkdir(parents=True, exist_ok=True)

    recent = get_recent_ratings(limit=50)
    summary = get_ratings_summary()

    lines = [
        "# Human Feedback for Agent Team",
        f"_Auto-updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}_",
        "",
        "## Summary",
        f"- Total ratings: {summary['total']}",
        f"- Approved: {summary.get('approved', 0)}",
        f"- Flagged for review: {summary.get('flagged', 0)}",
        f"- Marked incorrect: {summary.get('marked_incorrect', 0)}",
        f"- Avg rating: {summary.get('avg_rating', 'N/A')}",
        "",
        "## Recent Human Feedback",
        "",
    ]

    for r in recent:
        label = ACTION_LABELS.get(r.get("action"), "RATING")
        entity_desc = f'{r["entity_type"]} #{r["entity_id"]}'

        # Build metadata context string
        meta = r.get("metadata") or {}
        context_parts = []
        if meta.get("ticker"):
            context_parts.append(meta["ticker"])
        if meta.get("title"):
            context_parts.append(f'"{meta["title"]}"')
        context = f" ({', '.join(context_parts)})" if context_parts else ""

        # Rating stars
        rating_str = ""
        if r.get("rating") is not None:
            if r["rating"] in (-1, 0, 1):
                rating_str = {-1: " [thumbs-down]", 0: " [neutral]", 1: " [thumbs-up]"}.get(
                    r["rating"], ""
                )
            else:
                rating_str = f" [{r['rating']}/5 stars]"

        # Dimension
        dim_str = f" [{r['dimension']}]" if r.get("dimension") and r["dimension"] != "overall" else ""

        # Comment
        comment_str = ""
        if r.get("comment"):
            comment_str = f' — Comment: "{r["comment"]}"'

        timestamp = r.get("created_at", "")[:16] if r.get("created_at") else ""

        lines.append(
            f"- [{label}] {entity_desc}{context}{rating_str}{dim_str}{comment_str} ({timestamp})"
        )

    lines.append("")
    lines.append("---")
    lines.append("## Instructions for Agents")
    lines.append("- **FLAG**: Human thinks this data point needs review. Investigate and fix if wrong.")
    lines.append("- **INCORRECT**: Human says this is wrong. Prioritize correction.")
    lines.append("- **APPROVE**: Human confirms this is accurate. Use as calibration anchor.")
    lines.append("- **NEEDS_EVIDENCE**: Human wants more supporting data. Collect additional evidence.")
    lines.append("- Ratings with comments are highest priority — read the comment for context.")
    lines.append("")

    HUMAN_FEEDBACK_PATH.write_text("\n".join(lines))
