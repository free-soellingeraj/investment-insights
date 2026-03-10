"""Supervisor agent — monitors all teams, prioritizes human input, relays across teams."""

import logging

from sqlalchemy import text

from ai_opportunity_index.storage.db import get_session
from .base import BaseAgent

logger = logging.getLogger(__name__)

TEAM_CHANNELS = ["#core-optimization", "#ui-team", "#data-integrity"]


class Supervisor(BaseAgent):
    def __init__(self):
        super().__init__(agent_name="supervisor", role="supervisor", team_name=None)
        self._status_cycle = 0
        self._relayed_human_ids: set[int] = set()

    async def run_cycle(self, cycle_num: int):
        # PRIORITY 1: Check ALL channels for human input and broadcast to all teams
        for ch in TEAM_CHANNELS:
            msgs = self.read_others_messages(ch)
            for msg in msgs:
                if msg.get("message_type") == "human_input" and msg["id"] not in self._relayed_human_ids:
                    self._relayed_human_ids.add(msg["id"])
                    human_content = msg["content"]

                    # Broadcast to ALL other team channels
                    for target_ch in TEAM_CHANNELS:
                        if target_ch != ch:
                            self.post_message(
                                target_ch,
                                f"[Supervisor — Human Priority] A human posted in {ch}: "
                                f'"{human_content[:200]}". '
                                f"All teams should review this and act if relevant to your domain.",
                                msg_type="system",
                            )

                    # Also post to cross-team
                    self.post_message(
                        "#cross-team",
                        f"[Supervisor] Human input received in {ch}: "
                        f'"{human_content[:150]}". Broadcasting to all teams.',
                        msg_type="system",
                    )
                    logger.info("[supervisor] Relayed human input from %s to all teams", ch)

        # PRIORITY 2: Monitor cross-team channel for escalations
        cross_msgs = self.read_others_messages("#cross-team")
        for msg in cross_msgs:
            if msg.get("message_type") == "escalation":
                source = msg["sender_name"]
                # Broadcast to teams that didn't send it
                for ch in TEAM_CHANNELS:
                    team = ch.replace("#", "")
                    if team not in source:
                        self.post_message(
                            ch,
                            f"[Supervisor] Cross-team update from {source}: {msg['content'][:180]}",
                            msg_type="system",
                        )

        # Every 10th cycle, post status
        self._status_cycle += 1
        if self._status_cycle % 10 == 0:
            self._post_status_summary()

    def _post_status_summary(self):
        """Post a summary of all team activity to cross-team channel."""
        session = get_session()
        try:
            status_rows = session.execute(
                text("SELECT status, COUNT(*) FROM agents GROUP BY status")
            ).fetchall()
            status_summary = ", ".join(f"{r[0]}={r[1]}" for r in status_rows)

            plan_rows = session.execute(
                text("SELECT status, COUNT(*) FROM agent_plans GROUP BY status")
            ).fetchall()
            plan_summary = ", ".join(f"{r[0]}={r[1]}" for r in plan_rows) if plan_rows else "none"

            project_rows = session.execute(
                text("SELECT status, COUNT(*) FROM agent_projects GROUP BY status")
            ).fetchall()
            project_summary = ", ".join(f"{r[0]}={r[1]}" for r in project_rows) if project_rows else "none"

            msg_count = session.execute(
                text("SELECT COUNT(*) FROM agent_messages WHERE created_at > NOW() - INTERVAL '10 minutes'")
            ).scalar()

            summary = (
                f"[Status Report] Agents: {status_summary}. "
                f"Plans: {plan_summary}. "
                f"Projects: {project_summary}. "
                f"Messages (10min): {msg_count}."
            )
            self.post_message("#cross-team", summary, msg_type="system")
            logger.info("[supervisor] %s", summary)
        except Exception as e:
            logger.error("[supervisor] Status summary error: %s", e)
        finally:
            session.close()
