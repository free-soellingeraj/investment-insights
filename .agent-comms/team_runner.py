#!/usr/bin/env python3
"""Multi-team agent runner — event-driven, single process.

Agents ONLY wake when new messages arrive on channels they subscribe to.
No fixed sleep intervals. A channel watcher polls the DB every 2s and
signals per-channel asyncio.Events.

Usage:
    /opt/homebrew/bin/python3.11 .agent-comms/team_runner.py
"""

import asyncio
import logging
import signal
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import text

from ai_opportunity_index.storage.db import get_session
from roles.investigator import Investigator
from roles.idea_guy import IdeaGuy
from roles.adversarial import Adversarial
from roles.engineer import Engineer
from roles.code_reviewer import CodeReviewer
from roles.supervisor import Supervisor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("/tmp/team_runner.log"),
    ],
)
logger = logging.getLogger("team_runner")

TEAMS = ["core-optimization", "ui-team", "data-integrity"]

ROLE_CLASSES = {
    "investigator": Investigator,
    "idea_guy": IdeaGuy,
    "adversarial": Adversarial,
    "engineer": Engineer,
    "code_reviewer": CodeReviewer,
}

# Which channels each role subscribes to (wakes on new messages in these)
ROLE_CHANNELS = {
    "investigator": lambda team: [f"#{team}"],
    "idea_guy": lambda team: [f"#{team}", "#cross-team"],
    "adversarial": lambda team: [f"#{team}"],
    "engineer": lambda team: [f"#{team}"],
    "code_reviewer": lambda team: [f"#{team}"],
    "supervisor": lambda _: ["#cross-team", "#core-optimization", "#ui-team", "#data-integrity"],
}

# Safety timeout — max seconds an agent can sleep before waking anyway
MAX_IDLE_SECONDS = 300

# Role-specific delays (seconds) — stagger so agents don't all fire at once
ROLE_DELAYS = {
    "investigator": 2,    # responds first to directives
    "adversarial": 6,     # waits for findings to appear
    "idea_guy": 10,       # waits to see findings + challenges before reacting
    "engineer": 5,
    "code_reviewer": 5,
    "supervisor": 4,      # monitors across teams
}

shutdown_event = asyncio.Event()

# Per-AGENT asyncio.Event — set when new messages arrive on a subscribed channel
# (Using per-agent events prevents one agent from clearing another's wake signal)
agent_events: dict[str, asyncio.Event] = {}  # agent_name → Event
agent_subscriptions: dict[str, list[str]] = {}  # agent_name → [channel_names]


def register_agent(agent_name: str, channels: list[str]) -> asyncio.Event:
    """Register an agent and return its personal wake event."""
    ev = asyncio.Event()
    agent_events[agent_name] = ev
    agent_subscriptions[agent_name] = channels
    return ev


def handle_signal(sig, frame):
    logger.info("Received signal %s, shutting down...", sig)
    shutdown_event.set()
    for ev in agent_events.values():
        ev.set()


async def channel_watcher():
    """Poll DB every 2s for new messages, signal channel events."""
    last_ids: dict[str, int] = {}

    session = get_session()
    try:
        rows = session.execute(text("""
            SELECT c.name, COALESCE(MAX(m.id), 0)
            FROM agent_channels c
            LEFT JOIN agent_messages m ON m.channel_id = c.id
            GROUP BY c.name
        """)).fetchall()
        for row in rows:
            last_ids[row[0]] = row[1]
    finally:
        session.close()

    logger.info("Channel watcher started (tracking %d channels)", len(last_ids))

    while not shutdown_event.is_set():
        try:
            session = get_session()
            try:
                rows = session.execute(text("""
                    SELECT c.name, COALESCE(MAX(m.id), 0)
                    FROM agent_channels c
                    LEFT JOIN agent_messages m ON m.channel_id = c.id
                    GROUP BY c.name
                """)).fetchall()

                for row in rows:
                    ch_name, max_id = row[0], row[1]
                    if max_id > last_ids.get(ch_name, 0):
                        last_ids[ch_name] = max_id
                        # Wake ALL agents subscribed to this channel
                        for ag_name, ag_channels in agent_subscriptions.items():
                            if ch_name in ag_channels:
                                agent_events[ag_name].set()
            finally:
                session.close()
        except Exception:
            logger.exception("Channel watcher error")

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=2)
            break
        except asyncio.TimeoutError:
            pass


async def agent_loop(agent, role_name: str, subscribed_channels: list[str]):
    """Event-driven agent loop — only runs when channel events fire."""
    cycle = 0
    my_event = register_agent(agent.agent_name, subscribed_channels)
    agent.heartbeat()
    logger.info("Started %s (channels=%s)", agent.agent_name, subscribed_channels)

    # Run first cycle immediately to seed initial state
    cycle += 1
    try:
        await agent.run_cycle(cycle)
        agent.heartbeat()
        agent.increment_cycle()
    except Exception:
        logger.exception("Error in %s cycle %d", agent.agent_name, cycle)

    while not shutdown_event.is_set():
        # Clear MY event and wait for watcher to set it again
        my_event.clear()

        try:
            await asyncio.wait_for(my_event.wait(), timeout=MAX_IDLE_SECONDS)
        except asyncio.TimeoutError:
            pass  # idle timeout — run cycle anyway as heartbeat
        except asyncio.CancelledError:
            break

        if shutdown_event.is_set():
            break

        # Role-specific delay — stagger so agents react in natural order
        delay = ROLE_DELAYS.get(role_name, 5)
        await asyncio.sleep(delay)

        cycle += 1
        try:
            await agent.run_cycle(cycle)
            agent.heartbeat()
            agent.increment_cycle()
        except Exception:
            logger.exception("Error in %s cycle %d", agent.agent_name, cycle)
            try:
                agent.set_status("error")
            except Exception:
                pass

    agent.set_status("idle")
    logger.info("Stopped %s after %d cycles", agent.agent_name, cycle)


async def main():
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    logger.info("Starting event-driven agent runner (pid=%d)", os.getpid())

    tasks = []
    tasks.append(asyncio.create_task(channel_watcher()))

    for team in TEAMS:
        for role_name, RoleClass in ROLE_CLASSES.items():
            agent_name = f"{team}-{role_name}"
            agent = RoleClass(team_name=team, agent_name=agent_name)
            channels = ROLE_CHANNELS[role_name](team)
            tasks.append(asyncio.create_task(agent_loop(agent, role_name, channels)))

    supervisor = Supervisor()
    sup_channels = ROLE_CHANNELS["supervisor"]("")
    tasks.append(asyncio.create_task(agent_loop(supervisor, "supervisor", sup_channels)))

    logger.info("All %d agents + watcher started", len(tasks) - 1)
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("All agents stopped. Goodbye.")


if __name__ == "__main__":
    asyncio.run(main())
