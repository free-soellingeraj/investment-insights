"""Server-Sent Events (SSE) endpoints for real-time updates.

Each endpoint streams data changes to the frontend via text/event-stream.
The frontend connects with EventSource and receives JSON payloads.
"""

import asyncio
import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from litestar import get
from litestar.response import Stream
from sqlalchemy import text

from ai_opportunity_index.storage.db import get_session
from web.agents_api import (
    BULLETIN_PATH,
    PIPELINE_LOG,
    TASKS_DIR,
    PERSISTENT_AGENTS,
    _file_mtime_iso,
    _tail_lines,
    _read_file,
    _parse_bulletin,
    _parse_agent_sections,
    _get_recent_output_files,
    _guess_agent_name,
    _count_rate_limits,
    AGENT_DEFS,
    AGENT_NAMES,
    RESEARCHER_FINDINGS,
    build_agents_payload,
)

logger = logging.getLogger(__name__)

SSE_HEADERS = {
    "Cache-Control": "no-cache",
    "X-Accel-Buffering": "no",
    "Connection": "keep-alive",
}


def _safe_json(obj: object) -> str:
    """Serialize to JSON, converting datetimes to ISO strings."""

    def _default(o):
        if isinstance(o, datetime):
            return o.isoformat()
        raise TypeError(f"Object of type {type(o)} is not JSON serializable")

    return json.dumps(obj, default=_default)


# ── 1. Agent status SSE ─────────────────────────────────────────────────


def _agents_fingerprint() -> str:
    """Return a hash of bulletin mtime + persistent log file mtimes."""
    parts: list[str] = []
    parts.append(_file_mtime_iso(BULLETIN_PATH) or "none")
    # Include persistent script log files
    for pinfo in PERSISTENT_AGENTS.values():
        parts.append(_file_mtime_iso(pinfo["log_file"]) or "none")
    # Legacy task output files
    try:
        if TASKS_DIR.exists():
            for f in sorted(TASKS_DIR.iterdir()):
                if f.is_file():
                    parts.append(_file_mtime_iso(f) or "none")
    except OSError:
        pass
    return hashlib.md5("|".join(parts).encode()).hexdigest()


def _build_agents_payload() -> dict:
    """Build the full agents payload."""
    return build_agents_payload()


@get("/api/sse/agents")
async def sse_agents() -> Stream:
    """Stream agent status updates when bulletin or agent files change."""

    async def generate():
        last_fingerprint = ""
        while True:
            try:
                fp = _agents_fingerprint()
                if fp != last_fingerprint:
                    last_fingerprint = fp
                    payload = _build_agents_payload()
                    yield f"event: agents\ndata: {_safe_json(payload)}\n\n"
            except Exception:
                logger.exception("SSE agents error")
            await asyncio.sleep(2)

    return Stream(generate(), media_type="text/event-stream", headers=SSE_HEADERS)


# ── 2. Pipeline log SSE ─────────────────────────────────────────────────


@get("/api/sse/pipeline")
async def sse_pipeline() -> Stream:
    """Stream new pipeline log lines as they appear."""

    async def generate():
        last_size = 0
        try:
            if PIPELINE_LOG.exists():
                last_size = PIPELINE_LOG.stat().st_size
        except OSError:
            pass

        while True:
            try:
                if PIPELINE_LOG.exists():
                    current_size = PIPELINE_LOG.stat().st_size
                    if current_size != last_size:
                        # Read new lines from end of file
                        lines = _tail_lines(PIPELINE_LOG, 50)
                        yield f"event: pipeline\ndata: {_safe_json({'lines': lines})}\n\n"
                        last_size = current_size
            except Exception:
                logger.exception("SSE pipeline error")
            await asyncio.sleep(2)

    return Stream(generate(), media_type="text/event-stream", headers=SSE_HEADERS)


# ── 3. Ratings SSE ──────────────────────────────────────────────────────


@get("/api/sse/ratings")
async def sse_ratings() -> Stream:
    """Stream new human ratings as they are submitted."""

    async def generate():
        last_check = datetime.utcnow()

        while True:
            try:
                session = get_session()
                try:
                    result = session.execute(
                        text(
                            "SELECT id, entity_type, entity_id, rating, dimension, "
                            "comment, action, created_at "
                            "FROM human_ratings "
                            "WHERE created_at > :since "
                            "ORDER BY created_at DESC LIMIT 10"
                        ),
                        {"since": last_check},
                    )
                    rows = result.fetchall()
                    columns = result.keys()
                finally:
                    session.close()

                if rows:
                    ratings = [dict(zip(columns, row)) for row in rows]
                    yield f"event: ratings\ndata: {_safe_json({'ratings': ratings})}\n\n"
                    last_check = datetime.utcnow()

            except Exception:
                logger.exception("SSE ratings error")
            await asyncio.sleep(3)

    return Stream(generate(), media_type="text/event-stream", headers=SSE_HEADERS)


# ── 4. Scores SSE ───────────────────────────────────────────────────────


@get("/api/sse/scores")
async def sse_scores() -> Stream:
    """Stream company score changes from the materialized view."""

    async def generate():
        last_hash = ""

        while True:
            try:
                session = get_session()
                try:
                    result = session.execute(
                        text(
                            "SELECT company_id, ticker, company_name, "
                            "ai_opportunity_score, scored_at "
                            "FROM latest_company_scores "
                            "ORDER BY scored_at DESC NULLS LAST "
                            "LIMIT 20"
                        )
                    )
                    rows = result.fetchall()
                    columns = result.keys()
                finally:
                    session.close()

                scores = [dict(zip(columns, row)) for row in rows]
                current_hash = hashlib.md5(
                    _safe_json(scores).encode()
                ).hexdigest()

                if current_hash != last_hash:
                    last_hash = current_hash
                    yield f"event: scores\ndata: {_safe_json({'scores': scores})}\n\n"

            except Exception:
                logger.exception("SSE scores error")
            await asyncio.sleep(10)

    return Stream(generate(), media_type="text/event-stream", headers=SSE_HEADERS)


# ── 5. Agent channel messages SSE ────────────────────────────────────────


@get("/api/sse/agent-chat/{channel_name:str}")
async def sse_agent_chat(channel_name: str) -> Stream:
    """Stream new messages for a specific agent channel."""
    if not channel_name.startswith("#"):
        channel_name = f"#{channel_name}"

    async def generate():
        last_id = 0
        while True:
            try:
                session = get_session()
                try:
                    result = session.execute(
                        text("""
                            SELECT m.id, m.sender_name, m.content, m.message_type, m.metadata, m.created_at,
                                   a.role as sender_role
                            FROM agent_messages m
                            LEFT JOIN agents a ON a.id = m.agent_id
                            JOIN agent_channels c ON c.id = m.channel_id
                            WHERE c.name = :channel AND m.id > :last_id
                            ORDER BY m.created_at ASC LIMIT 20
                        """),
                        {"channel": channel_name, "last_id": last_id},
                    )
                    rows = result.fetchall()
                    columns = result.keys()
                finally:
                    session.close()

                if rows:
                    messages = [dict(zip(columns, row)) for row in rows]
                    last_id = messages[-1]["id"]
                    yield f"event: agent-chat\ndata: {_safe_json({'messages': messages})}\n\n"
            except Exception:
                logger.exception("SSE agent-chat error")
            await asyncio.sleep(2)

    return Stream(generate(), media_type="text/event-stream", headers=SSE_HEADERS)
