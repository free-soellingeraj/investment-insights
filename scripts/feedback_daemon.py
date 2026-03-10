#!/usr/bin/env python3
"""Feedback Pipeline Daemon

Polls the human_ratings table and writes a summary to .agent-comms/human_feedback.md
so that the agent team can act on user feedback in real-time.

Also appends urgent items (flagged/incorrect) to the bulletin board Action Items.

Usage:
    python3.11 scripts/feedback_daemon.py          # run forever (15s poll)
    python3.11 scripts/feedback_daemon.py --once    # single pass then exit
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import text

from ai_opportunity_index.storage.db import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [feedback-daemon] %(levelname)s %(message)s",
)
logger = logging.getLogger("feedback-daemon")

AGENT_COMMS_DIR = PROJECT_ROOT / ".agent-comms"
FEEDBACK_FILE = AGENT_COMMS_DIR / "human_feedback.md"
BULLETIN_FILE = AGENT_COMMS_DIR / "bulletin.md"
POLL_INTERVAL = 15  # seconds

# Action categories that require agent intervention
URGENT_ACTIONS = {"flag", "incorrect", "mark_incorrect"}
NEEDS_EVIDENCE_ACTIONS = {"needs_evidence", "needs_more_evidence"}
APPROVE_ACTIONS = {"approve", "approved"}
COMMENT_ACTIONS = {"comment", None}  # NULL action = comment-only

# SQL to fetch ratings with entity context
RATINGS_QUERY = text("""
    SELECT
        hr.id,
        hr.entity_type,
        hr.entity_id,
        hr.rating,
        hr.dimension,
        hr.comment,
        hr.action,
        hr.metadata,
        hr.created_at,
        -- Join context depending on entity_type
        CASE
            WHEN hr.entity_type = 'project' THEN ip.short_title
            WHEN hr.entity_type = 'valuation' THEN v.narrative
            WHEN hr.entity_type = 'company' THEN c.company_name
            WHEN hr.entity_type = 'evidence_group' THEN eg.representative_text
            ELSE NULL
        END AS entity_label,
        CASE
            WHEN hr.entity_type = 'project' THEN c_proj.ticker
            WHEN hr.entity_type = 'valuation' THEN c_val.ticker
            WHEN hr.entity_type = 'company' THEN c.ticker
            WHEN hr.entity_type = 'evidence_group' THEN c_eg.ticker
            ELSE NULL
        END AS ticker,
        CASE
            WHEN hr.entity_type = 'valuation' THEN v.group_id
            ELSE NULL
        END AS group_id
    FROM human_ratings hr
    LEFT JOIN investment_projects ip ON hr.entity_type = 'project' AND hr.entity_id = ip.id
    LEFT JOIN companies c_proj ON ip.company_id = c_proj.id
    LEFT JOIN valuations v ON hr.entity_type = 'valuation' AND hr.entity_id = v.id
    LEFT JOIN evidence_groups eg_val ON v.group_id = eg_val.id
    LEFT JOIN companies c_val ON eg_val.company_id = c_val.id
    LEFT JOIN companies c ON hr.entity_type = 'company' AND hr.entity_id = c.id
    LEFT JOIN evidence_groups eg ON hr.entity_type = 'evidence_group' AND hr.entity_id = eg.id
    LEFT JOIN companies c_eg ON eg.company_id = c_eg.id
    ORDER BY hr.created_at DESC
""")

# Stats query
STATS_QUERY = text("""
    SELECT
        count(*) AS total,
        count(*) FILTER (WHERE action IN ('flag', 'incorrect', 'mark_incorrect')) AS flagged,
        count(*) FILTER (WHERE action IN ('approve', 'approved')) AS approved,
        count(*) FILTER (WHERE action IN ('needs_evidence', 'needs_more_evidence')) AS needs_evidence,
        count(*) FILTER (WHERE action = 'comment' OR action IS NULL) AS comments,
        min(created_at) AS earliest,
        max(created_at) AS latest
    FROM human_ratings
""")

# Per-company approval rate
APPROVAL_RATE_QUERY = text("""
    SELECT
        c.ticker,
        c.company_name,
        count(*) FILTER (WHERE hr.action IN ('approve', 'approved')) AS approved_count,
        count(*) AS total_count,
        ROUND(
            100.0 * count(*) FILTER (WHERE hr.action IN ('approve', 'approved'))
            / NULLIF(count(*), 0),
            1
        ) AS approval_pct
    FROM human_ratings hr
    JOIN investment_projects ip ON hr.entity_type = 'project' AND hr.entity_id = ip.id
    JOIN companies c ON ip.company_id = c.id
    GROUP BY c.ticker, c.company_name
    HAVING count(*) >= 2
    ORDER BY approval_pct ASC
    LIMIT 20
""")


def categorize_action(action: str | None) -> str:
    """Map action string to category."""
    if action in URGENT_ACTIONS:
        return "urgent"
    if action in NEEDS_EVIDENCE_ACTIONS:
        return "needs_evidence"
    if action in APPROVE_ACTIONS:
        return "approved"
    return "comment"


def format_timestamp(dt: datetime | None) -> str:
    if dt is None:
        return "unknown"
    return dt.strftime("%Y-%m-%d %H:%M")


def format_rating_line(row) -> str:
    """Format a single rating row into a bullet point."""
    action = row.action or "comment"
    tag = action.upper().replace("_", " ")
    entity_type = row.entity_type
    entity_id = row.entity_id
    label = row.entity_label or ""
    ticker = row.ticker or ""
    comment = row.comment or ""
    ts = format_timestamp(row.created_at)
    group_id = row.group_id

    # Truncate long labels/comments
    if len(label) > 80:
        label = label[:77] + "..."
    if len(comment) > 120:
        comment = comment[:117] + "..."

    ticker_str = f" ({ticker})" if ticker else ""
    label_str = f' "{label}"' if label else ""
    comment_str = f' — "{comment}"' if comment else ""
    group_str = f" group={group_id}" if group_id else ""

    return (
        f"- [{tag}] {entity_type.capitalize()} id={entity_id}{group_str}"
        f"{label_str}{ticker_str}{comment_str} — rated at {ts}"
    )


def generate_feedback_markdown(engine) -> str:
    """Query human_ratings and produce the feedback markdown."""
    with engine.connect() as conn:
        rows = conn.execute(RATINGS_QUERY).fetchall()
        stats = conn.execute(STATS_QUERY).fetchone()
        approval_rates = conn.execute(APPROVAL_RATE_QUERY).fetchall()

    # Categorize
    urgent = []
    approved = []
    needs_evidence = []
    comments = []

    for row in rows:
        cat = categorize_action(row.action)
        line = format_rating_line(row)
        if cat == "urgent":
            urgent.append(line)
        elif cat == "approved":
            approved.append(line)
        elif cat == "needs_evidence":
            needs_evidence.append(line)
        else:
            comments.append(line)

    # Build markdown
    parts = [
        "# Human Feedback Queue",
        f"_Last updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC_",
        "",
    ]

    parts.append("## Urgent (Flagged / Marked Incorrect)")
    if urgent:
        parts.extend(urgent)
    else:
        parts.append("_None — all clear._")
    parts.append("")

    parts.append("## Needs More Evidence")
    if needs_evidence:
        parts.extend(needs_evidence)
    else:
        parts.append("_None._")
    parts.append("")

    parts.append("## Approved (No Action Needed)")
    if approved:
        parts.extend(approved)
    else:
        parts.append("_None yet._")
    parts.append("")

    parts.append("## Comments (Info Only)")
    if comments:
        parts.extend(comments)
    else:
        parts.append("_None._")
    parts.append("")

    # Stats
    parts.append("## Stats")
    if stats and stats.total > 0:
        parts.append(f"- Total ratings: {stats.total}")
        parts.append(f"- Flagged/Incorrect: {stats.flagged}")
        parts.append(f"- Approved: {stats.approved}")
        parts.append(f"- Needs more evidence: {stats.needs_evidence}")
        parts.append(f"- Comments: {stats.comments}")
        parts.append(f"- Date range: {format_timestamp(stats.earliest)} to {format_timestamp(stats.latest)}")
    else:
        parts.append("- Total ratings: 0")
        parts.append("_No ratings submitted yet._")
    parts.append("")

    # Approval rates by company
    if approval_rates:
        parts.append("## Approval Rate by Company (min 2 ratings)")
        for ar in approval_rates:
            parts.append(
                f"- {ar.ticker or 'N/A'} ({ar.company_name or 'Unknown'}): "
                f"{ar.approved_count}/{ar.total_count} = {ar.approval_pct}%"
            )
        parts.append("")

    # Agent instructions
    parts.append("## Agent Instructions")
    parts.append("- **Guardian**: Fix flagged dollar estimates and incorrect classifications immediately.")
    parts.append("- **Engineers**: Implement data corrections for 'Mark Incorrect' items.")
    parts.append("- **Researcher**: Note 'Needs More Evidence' items — these need better data collection.")
    parts.append("- **Architect**: Review patterns in flagged items for systematic issues.")
    parts.append("")

    return "\n".join(parts)


def append_urgent_to_bulletin(urgent_items: list[str]) -> None:
    """Append urgent feedback items to the bulletin board Action Items section."""
    if not urgent_items or not BULLETIN_FILE.exists():
        return

    bulletin = BULLETIN_FILE.read_text()

    # Build action items from urgent feedback
    action_lines = []
    for item in urgent_items:
        # Parse the item to route to the right agent
        if "dollar" in item.lower() or "estimate" in item.lower():
            action_lines.append(f"FOR Data Quality Guardian: {item}")
        elif "incorrect" in item.lower() or "classification" in item.lower():
            action_lines.append(f"FOR Backend Engineer: {item}")
        else:
            action_lines.append(f"FOR Data Quality Guardian: {item}")

    # Find the Action Items section and append
    marker = "## Action Items (cross-agent)"
    if marker in bulletin:
        # Check if we already added these (avoid duplicates)
        new_lines = []
        for line in action_lines:
            # Use a simple fingerprint (entity id) to deduplicate
            if line not in bulletin:
                new_lines.append(line)

        if new_lines:
            insert_text = "\n\n" + "\n\n".join(new_lines)
            bulletin = bulletin + insert_text
            BULLETIN_FILE.write_text(bulletin)
            logger.info("Appended %d urgent items to bulletin board", len(new_lines))
    else:
        logger.warning("No Action Items section found in bulletin board")


def run_once(engine) -> tuple[str, list[str]]:
    """Single poll cycle. Returns (markdown, urgent_items)."""
    md = generate_feedback_markdown(engine)

    # Extract urgent items for bulletin
    urgent = []
    for line in md.split("\n"):
        if line.startswith("- [FLAG]") or line.startswith("- [INCORRECT]") or line.startswith("- [MARK INCORRECT]"):
            urgent.append(line)

    return md, urgent


def main():
    parser = argparse.ArgumentParser(description="Human Feedback Pipeline Daemon")
    parser.add_argument("--once", action="store_true", help="Run a single cycle and exit")
    args = parser.parse_args()

    # Ensure agent-comms directory exists
    AGENT_COMMS_DIR.mkdir(exist_ok=True)

    engine = get_engine()
    logger.info("Feedback daemon starting (poll interval: %ds)", POLL_INTERVAL)

    last_urgent_count = 0

    while True:
        try:
            md, urgent = run_once(engine)

            # Write feedback file atomically
            tmp_path = FEEDBACK_FILE.with_suffix(".tmp")
            tmp_path.write_text(md)
            tmp_path.rename(FEEDBACK_FILE)
            logger.info(
                "Updated human_feedback.md (%d urgent, %d total lines)",
                len(urgent), md.count("\n"),
            )

            # Only append to bulletin when new urgent items appear
            if len(urgent) > last_urgent_count:
                new_urgent = urgent[: len(urgent) - last_urgent_count]
                append_urgent_to_bulletin(new_urgent)
            last_urgent_count = len(urgent)

        except Exception:
            logger.exception("Error in feedback daemon cycle")

        if args.once:
            break
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
