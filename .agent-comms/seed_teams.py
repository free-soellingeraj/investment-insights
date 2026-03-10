"""Seed agent teams, channels, and agents into the database.

Idempotent — safe to run multiple times (uses ON CONFLICT DO NOTHING).
"""

from sqlalchemy import text

from ai_opportunity_index.storage.db import get_session

TEAMS = [
    ("core-optimization", "Core Optimization", "Scoring pipeline, calibration, and data integrity"),
    ("ui-team", "UI Team", "Frontend, GraphQL, and user experience"),
    ("data-integrity", "Data Integrity", "Evidence quality, provenance, and cross-source verification"),
]

CHANNELS = [
    ("#core-optimization", "team", "Core optimization team channel"),
    ("#ui-team", "team", "UI team channel"),
    ("#data-integrity", "team", "Data integrity team channel"),
    ("#cross-team", "cross", "Cross-team coordination channel"),
]

# 5 roles per team + 1 supervisor (no team)
ROLES = ["investigator", "idea_guy", "adversarial", "engineer", "code_reviewer"]

AGENTS = []
for team_name, _, _ in TEAMS:
    for role in ROLES:
        agent_name = f"{team_name}-{role}"
        display = f"{role.replace('_', ' ').title()} ({team_name})"
        AGENTS.append((agent_name, team_name, role, display))

# Supervisor has no team
AGENTS.append(("supervisor", None, "supervisor", "Supervisor"))


def seed():
    session = get_session()
    try:
        # Seed teams
        for name, display_name, description in TEAMS:
            session.execute(text(
                "INSERT INTO agent_teams (name, display_name, description) "
                "VALUES (:name, :display_name, :description) "
                "ON CONFLICT (name) DO NOTHING"
            ), {"name": name, "display_name": display_name, "description": description})

        # Seed channels
        for name, channel_type, description in CHANNELS:
            session.execute(text(
                "INSERT INTO agent_channels (name, channel_type, description) "
                "VALUES (:name, :channel_type, :description) "
                "ON CONFLICT (name) DO NOTHING"
            ), {"name": name, "channel_type": channel_type, "description": description})

        # Seed agents — need team_id lookup
        for agent_name, team_name, role, display_name in AGENTS:
            if team_name is not None:
                result = session.execute(text(
                    "SELECT id FROM agent_teams WHERE name = :name"
                ), {"name": team_name}).fetchone()
                team_id = result[0] if result else None
            else:
                team_id = None

            # Check if agent already exists (no unique constraint on name alone)
            existing = session.execute(text(
                "SELECT id FROM agents WHERE name = :name"
            ), {"name": agent_name}).fetchone()

            if existing is None:
                session.execute(text(
                    "INSERT INTO agents (name, team_id, role, display_name) "
                    "VALUES (:name, :team_id, :role, :display_name)"
                ), {
                    "name": agent_name,
                    "team_id": team_id,
                    "role": role,
                    "display_name": display_name,
                })

        session.commit()
        print(f"Seeded {len(TEAMS)} teams, {len(CHANNELS)} channels, {len(AGENTS)} agents.")
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    seed()
