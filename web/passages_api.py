"""API endpoint for evidence group passages.

GET /api/passages/{group_id} — get all passages for an evidence group
"""

import logging

from litestar import get
from sqlalchemy import text

from ai_opportunity_index.storage.db import get_session

logger = logging.getLogger(__name__)


@get("/api/passages/{group_id:int}")
async def api_passages(group_id: int) -> dict:
    """Return all passages for a given evidence group, ordered by confidence descending."""
    try:
        with get_session() as session:
            rows = session.execute(
                text("""
                    SELECT id, group_id, passage_text, source_type, source_url,
                           source_publisher, source_date, source_authority,
                           confidence, capture_stage, target_dimension,
                           source_author, source_author_role, reasoning
                    FROM (
                        SELECT DISTINCT ON (passage_text)
                               id, group_id, passage_text, source_type, source_url,
                               source_publisher, source_date, source_authority,
                               confidence, capture_stage, target_dimension,
                               source_author, source_author_role, reasoning
                        FROM evidence_group_passages
                        WHERE group_id = :group_id
                        ORDER BY passage_text, confidence DESC
                    ) deduped
                    ORDER BY confidence DESC
                """),
                {"group_id": group_id},
            ).mappings().all()

            passages = []
            for r in rows:
                passages.append({
                    "id": r["id"],
                    "groupId": r["group_id"],
                    "passageText": r["passage_text"],
                    "sourceType": r["source_type"],
                    "sourceUrl": r["source_url"],
                    "sourcePublisher": r["source_publisher"],
                    "sourceDate": str(r["source_date"]) if r["source_date"] else None,
                    "sourceAuthority": r["source_authority"],
                    "confidence": r["confidence"],
                    "captureStage": r["capture_stage"],
                    "targetDimension": r["target_dimension"],
                    "sourceAuthor": r["source_author"],
                    "sourceAuthorRole": r["source_author_role"],
                    "reasoning": r["reasoning"],
                })

            return {"passages": passages}
    except Exception as e:
        logger.exception("Failed to fetch passages for group %d", group_id)
        return {"passages": [], "error": str(e)}
