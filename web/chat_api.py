"""API endpoints for floating chat widget — issue/feedback capture.

POST /api/chat            — save a chat message with automatic context
GET  /api/chat            — retrieve recent messages
POST /api/chat/addressed  — toggle addressed status on a message
"""

import json
import logging
from datetime import datetime
from pathlib import Path

from litestar import Request, get, post
from sqlalchemy import text

from ai_opportunity_index.storage.db import get_session

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
AGENT_COMMS_DIR = PROJECT_ROOT / ".agent-comms"
HUMAN_CHAT_PATH = AGENT_COMMS_DIR / "human_chat.md"

_TABLE_ENSURED = False


def _ensure_table():
    """Create chat_messages table if it doesn't exist."""
    global _TABLE_ENSURED
    if _TABLE_ENSURED:
        return
    s = get_session()
    try:
        s.execute(text("""
            CREATE TABLE IF NOT EXISTS chat_messages (
                id SERIAL PRIMARY KEY,
                message TEXT NOT NULL,
                context JSONB DEFAULT '{}',
                page_url TEXT,
                ticker TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """))
        s.commit()
        _TABLE_ENSURED = True
    except Exception:
        s.rollback()
        raise
    finally:
        s.close()


@post("/api/chat")
async def api_chat_create(request: Request) -> dict:
    """Save a chat message with page context."""
    _ensure_table()
    try:
        body = await request.json()
    except Exception:
        return {"ok": False, "error": "Invalid JSON body"}

    message = body.get("message", "").strip()
    if not message:
        return {"ok": False, "error": "message is required"}

    context = body.get("context", {})
    page_url = body.get("page_url", context.get("url", ""))
    ticker = body.get("ticker", context.get("ticker", ""))

    s = get_session()
    try:
        result = s.execute(
            text("""
                INSERT INTO chat_messages (message, context, page_url, ticker, created_at, addressed)
                VALUES (:message, :context, :page_url, :ticker, NOW(), FALSE)
                RETURNING id, message, context, page_url, ticker, created_at, addressed
            """),
            {
                "message": message,
                "context": json.dumps(context) if isinstance(context, dict) else "{}",
                "page_url": page_url,
                "ticker": ticker,
            },
        )
        row = result.mappings().fetchone()
        s.commit()

        saved = {
            "id": row["id"],
            "message": row["message"],
            "context": row["context"] if isinstance(row["context"], dict) else json.loads(row["context"] or "{}"),
            "page_url": row["page_url"],
            "ticker": row["ticker"],
            "created_at": row["created_at"].isoformat() if row["created_at"] else None,
            "addressed": bool(row["addressed"]),
        }

        # Update agent-comms file
        try:
            _update_agent_chat_file()
        except Exception as e:
            logger.warning("Failed to update agent chat file: %s", e)

        return {"ok": True, "message": saved}
    except Exception as e:
        s.rollback()
        logger.error("Error saving chat message: %s", e)
        return {"ok": False, "error": "Internal error saving message"}
    finally:
        s.close()


@get("/api/chat")
async def api_chat_list(request: Request) -> dict:
    """Retrieve recent chat messages."""
    _ensure_table()
    limit = int(request.query_params.get("limit", "100"))
    limit = min(limit, 500)

    s = get_session()
    try:
        result = s.execute(
            text("""
                SELECT id, message, context, page_url, ticker, created_at, addressed, response_thread
                FROM chat_messages
                ORDER BY created_at DESC
                LIMIT :limit
            """),
            {"limit": limit},
        )
        rows = result.mappings().fetchall()
        messages = []
        for row in rows:
            messages.append({
                "id": row["id"],
                "message": row["message"],
                "context": row["context"] if isinstance(row["context"], dict) else json.loads(row["context"] or "{}"),
                "page_url": row["page_url"],
                "ticker": row["ticker"],
                "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                "addressed": bool(row["addressed"]),
                "response_thread": row.get("response_thread") or [],
            })
        # Reverse so oldest is first (chat order)
        messages.reverse()
        return {"ok": True, "messages": messages, "count": len(messages)}
    except Exception as e:
        logger.error("Error fetching chat messages: %s", e)
        return {"ok": False, "error": str(e)}
    finally:
        s.close()


@post("/api/chat/addressed")
async def api_chat_toggle_addressed(request: Request) -> dict:
    """Toggle the addressed status of a chat message."""
    _ensure_table()
    try:
        body = await request.json()
    except Exception:
        return {"ok": False, "error": "Invalid JSON body"}

    msg_id = body.get("id")
    if msg_id is None:
        return {"ok": False, "error": "id is required"}

    addressed = body.get("addressed")  # explicit value, or toggle if None

    s = get_session()
    try:
        if addressed is None:
            # Toggle
            result = s.execute(
                text("""
                    UPDATE chat_messages SET addressed = NOT addressed
                    WHERE id = :id
                    RETURNING id, addressed
                """),
                {"id": int(msg_id)},
            )
        else:
            result = s.execute(
                text("""
                    UPDATE chat_messages SET addressed = :addressed
                    WHERE id = :id
                    RETURNING id, addressed
                """),
                {"id": int(msg_id), "addressed": bool(addressed)},
            )
        row = result.mappings().fetchone()
        s.commit()

        if not row:
            return {"ok": False, "error": "Message not found"}

        return {"ok": True, "id": row["id"], "addressed": bool(row["addressed"])}
    except Exception as e:
        s.rollback()
        logger.error("Error toggling addressed: %s", e)
        return {"ok": False, "error": str(e)}
    finally:
        s.close()


def _update_agent_chat_file():
    """Write recent chat messages to .agent-comms/human_chat.md for agents to read."""
    AGENT_COMMS_DIR.mkdir(parents=True, exist_ok=True)

    s = get_session()
    try:
        result = s.execute(
            text("""
                SELECT id, message, context, page_url, ticker, created_at, addressed
                FROM chat_messages
                ORDER BY created_at DESC
                LIMIT 50
            """)
        )
        rows = result.mappings().fetchall()
    finally:
        s.close()

    lines = [
        "# Human Chat Messages",
        f"_Auto-updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}_",
        "",
        "## Recent Messages",
        "",
    ]

    for row in reversed(list(rows)):
        ts = row["created_at"].strftime("%Y-%m-%d %H:%M") if row["created_at"] else ""
        ticker_str = f" [{row['ticker']}]" if row["ticker"] else ""
        page_str = f" on {row['page_url']}" if row["page_url"] else ""
        check = " [ADDRESSED]" if row["addressed"] else ""
        lines.append(f"- **{ts}**{ticker_str}{page_str}: {row['message']}{check}")

    lines.append("")
    lines.append("---")
    lines.append("## Instructions for Agents")
    lines.append("- These are messages from the human user describing issues, feedback, or observations.")
    lines.append("- Messages with a ticker context relate to a specific company's data.")
    lines.append("- Prioritize messages that describe bugs or data quality issues.")
    lines.append("- Messages marked [ADDRESSED] have been resolved — no action needed.")
    lines.append("")

    HUMAN_CHAT_PATH.write_text("\n".join(lines))
