"""Base class for agent roles."""

import json
import logging
import os

from sqlalchemy import text

from ai_opportunity_index.storage.db import get_session

logger = logging.getLogger(__name__)


class BaseAgent:
    """Base class providing heartbeat, message I/O, and cycle tracking."""

    def __init__(self, agent_name: str, role: str, team_name: str | None = None):
        self.agent_name = agent_name
        self.role = role
        self.team_name = team_name  # None for supervisor
        self._agent_id: int | None = None
        # Track last seen message ID per channel — eliminates timing gaps
        self._last_seen: dict[str, int] = {}

    @property
    def agent_id(self) -> int:
        if self._agent_id is None:
            session = get_session()
            try:
                row = session.execute(
                    text("SELECT id FROM agents WHERE name = :name"),
                    {"name": self.agent_name},
                ).fetchone()
                if row:
                    self._agent_id = row[0]
                else:
                    raise RuntimeError(f"Agent {self.agent_name} not found in DB. Run seed_teams.py first.")
            finally:
                session.close()
        return self._agent_id

    def heartbeat(self):
        """Update last_heartbeat and pid in the DB."""
        session = get_session()
        try:
            session.execute(
                text(
                    "UPDATE agents SET last_heartbeat = NOW(), pid = :pid, status = 'running' "
                    "WHERE id = :id"
                ),
                {"pid": os.getpid(), "id": self.agent_id},
            )
            session.commit()
        finally:
            session.close()

    def increment_cycle(self):
        """Increment cycle_count."""
        session = get_session()
        try:
            session.execute(
                text("UPDATE agents SET cycle_count = cycle_count + 1 WHERE id = :id"),
                {"id": self.agent_id},
            )
            session.commit()
        finally:
            session.close()

    def increment_fix(self):
        """Increment fix_count."""
        session = get_session()
        try:
            session.execute(
                text("UPDATE agents SET fix_count = fix_count + 1 WHERE id = :id"),
                {"id": self.agent_id},
            )
            session.commit()
        finally:
            session.close()

    def post_message(self, channel_name: str, content: str, msg_type: str = "chat", metadata: dict | None = None):
        """Post a message to a channel."""
        session = get_session()
        try:
            session.execute(
                text("""
                    INSERT INTO agent_messages (channel_id, agent_id, sender_name, content, message_type, metadata, created_at)
                    SELECT c.id, :agent_id, :sender, :content, :msg_type, CAST(:meta AS jsonb), NOW()
                    FROM agent_channels c WHERE c.name = :channel
                """),
                {
                    "agent_id": self.agent_id,
                    "sender": self.agent_name,
                    "content": content,
                    "msg_type": msg_type,
                    "meta": json.dumps(metadata) if metadata else None,
                    "channel": channel_name,
                },
            )
            session.commit()
        finally:
            session.close()

    def read_new_messages(self, channel_name: str, limit: int = 50) -> list[dict]:
        """Read messages newer than last seen ID for this channel.

        This eliminates timing-gap issues — every message is seen exactly once.
        """
        last_id = self._last_seen.get(channel_name, 0)
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
                    ORDER BY m.created_at ASC LIMIT :limit
                """),
                {"channel": channel_name, "last_id": last_id, "limit": limit},
            )
            messages = [dict(row._mapping) for row in result.fetchall()]
            if messages:
                self._last_seen[channel_name] = messages[-1]["id"]
            return messages
        finally:
            session.close()

    def read_others_messages(self, channel_name: str, limit: int = 50) -> list[dict]:
        """Read new messages from others (not self) on a channel."""
        msgs = self.read_new_messages(channel_name, limit)
        return [m for m in msgs if m["sender_name"] != self.agent_name]

    def set_status(self, status: str):
        """Set agent status."""
        session = get_session()
        try:
            session.execute(
                text("UPDATE agents SET status = :status WHERE id = :id"),
                {"status": status, "id": self.agent_id},
            )
            session.commit()
        finally:
            session.close()

    async def run_cycle(self, cycle_num: int):
        """Override in subclasses. Called each cycle."""
        raise NotImplementedError
