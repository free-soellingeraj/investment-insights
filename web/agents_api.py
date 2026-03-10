"""API endpoints for monitoring the agent team in real-time."""

import logging
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from litestar import get, post, put, Request
from sqlalchemy import text as sa_text

from ai_opportunity_index.storage.db import get_session as _get_db_session

logger = logging.getLogger(__name__)

# Make agent-comms importable for pr_helper
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / ".agent-comms"))
from roles.pr_helper import merge_pr as _merge_pr

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BULLETIN_PATH = PROJECT_ROOT / ".agent-comms" / "bulletin.md"
HUMAN_CHAT_PATH = PROJECT_ROOT / ".agent-comms" / "human_chat.md"
TASKS_DIR = Path(
    "/private/tmp/claude-502/"
    "-Users-free-soellingeraj-code--para-llm-directory-envs-"
    "investment-insights-rime-related-alerts-investment-insights/tasks"
)
PIPELINE_LOG = Path("/tmp/parallel_pipeline.log")
MONITOR_LOG = Path("/tmp/pipeline_monitor.log")
RESEARCHER_FINDINGS = Path("/tmp/researcher_findings.md")

# Persistent monitor scripts and their log files
PERSISTENT_AGENTS: dict[str, dict] = {
    "data_quality_guardian": {
        "name": "Data Quality Guardian",
        "log_file": Path("/tmp/guardian_loop.log"),
        "script": PROJECT_ROOT / ".agent-comms" / "guardian_loop.py",
        "pgrep_pattern": "guardian_loop",
    },
    "pipeline_optimizer": {
        "name": "Pipeline Optimizer",
        "log_file": Path("/tmp/pipeline_monitor_loop.log"),
        "script": PROJECT_ROOT / ".agent-comms" / "pipeline_monitor_loop.py",
        "pgrep_pattern": "pipeline_monitor_loop",
    },
    "bottleneck_researcher": {
        "name": "Bottleneck Researcher",
        "log_file": Path("/tmp/sre_monitor_loop.log"),
        "script": PROJECT_ROOT / ".agent-comms" / "sre_monitor_loop.py",
        "pgrep_pattern": "sre_monitor_loop",
    },
}

# Maps display name -> snake_case key used by the frontend
AGENT_DEFS: dict[str, dict] = {
    "Data Quality Guardian": {
        "key": "data_quality_guardian",
        "role": "Validates data integrity, detects anomalies, enforces quality gates",
    },
    "Pipeline Optimizer": {
        "key": "pipeline_optimizer",
        "role": "Optimizes pipeline throughput, parallelism, and resource usage",
    },
    "Code Quality Architect": {
        "key": "code_quality_architect",
        "role": "Refactors code, enforces standards, improves maintainability",
    },
    "Bottleneck Researcher": {
        "key": "bottleneck_researcher",
        "role": "Investigates performance bottlenecks, profiles slow paths",
    },
}

AGENT_NAMES = list(AGENT_DEFS.keys())


def _read_file(path: Path) -> str:
    """Read a file, returning empty string if missing or unreadable."""
    try:
        return path.read_text(errors="replace")
    except (OSError, IOError):
        return ""


def _tail_lines(path: Path, n: int) -> list[str]:
    """Read the last n lines of a file, returning a list of strings."""
    try:
        with open(path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            if size == 0:
                return []
            chunk_size = min(size, 65536)
            f.seek(-chunk_size, 2)
            data = f.read().decode("utf-8", errors="replace")
            lines = data.splitlines()
            return lines[-n:]
    except (OSError, IOError):
        return []


def _file_mtime_iso(path: Path) -> str | None:
    """Return ISO-8601 mtime of a file, or None."""
    try:
        ts = path.stat().st_mtime
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    except (OSError, IOError):
        return None


def _parse_action_items(raw_section: str) -> list[dict]:
    """Parse the action items section into structured objects.

    Each item starts with "FOR <agent>" or "FROM DASHBOARD:" and may span
    multiple lines until the next item or end of string.  We detect resolved
    items via markers like **ANSWERED**, **DONE**, **FIXED**, **INVESTIGATED**.
    Priority is elevated for [FLAG] / [INCORRECT] tags.
    """
    if not raw_section:
        return []

    # Split into individual items.  Each starts with FOR/FROM at column 0.
    item_re = re.compile(
        r"^(FOR\s+.+?:|FROM\s+DASHBOARD:)",
        re.MULTILINE,
    )
    splits = list(item_re.finditer(raw_section))
    if not splits:
        return []

    items: list[dict] = []
    for idx, m in enumerate(splits):
        start = m.start()
        end = splits[idx + 1].start() if idx + 1 < len(splits) else len(raw_section)
        full_text = raw_section[start:end].strip()
        if not full_text:
            continue

        header = m.group(1)

        # Assignee
        assignee = ""
        source = ""
        am = re.match(r"FOR\s+(.+?)(?:\s*\(from\s+(.+?)\))?\s*:", header)
        if am:
            assignee = am.group(1).strip()
            source = (am.group(2) or "").strip()
        elif header.startswith("FROM DASHBOARD"):
            assignee = "All"
            source = "Dashboard"

        body = full_text[len(header):].strip()

        # Strip the preamble line that explains the format
        if body.startswith("_Items that need"):
            body = re.sub(r"^_.*?_\s*", "", body)
        if not body:
            continue

        # Status detection
        resolved_markers = ("**ANSWERED", "**DONE", "**FIXED", "**INVESTIGATED")
        status = "resolved" if any(mk in full_text for mk in resolved_markers) else "open"

        # Priority detection
        priority = "normal"
        if "[FLAG]" in full_text or "[INCORRECT]" in full_text:
            priority = "high"
        elif "CRITICAL" in full_text or "RESTART NEEDED" in full_text:
            priority = "critical"

        # Extract a short summary (first sentence or up to 120 chars)
        summary_text = body.split("\n")[0]
        # Remove markdown bold markers for cleaner display
        summary_text = summary_text.replace("**", "")
        if len(summary_text) > 140:
            summary_text = summary_text[:137] + "..."

        # Cycle/timestamp info
        cycle = ""
        cm = re.search(r"Cycle\s+(\d+)", full_text)
        if cm:
            cycle = cm.group(0)
        ts = ""
        tm = re.search(r"rated at (\d{4}-\d{2}-\d{2} \d{2}:\d{2})", full_text)
        if tm:
            ts = tm.group(1)

        items.append({
            "assignee": assignee,
            "source": source,
            "summary": summary_text,
            "status": status,
            "priority": priority,
            "cycle": cycle,
            "timestamp": ts,
            "raw": body,
        })

    return items


def _parse_bulletin(raw: str) -> dict:
    """Extract stats, action items, and claims from bulletin markdown."""
    latest_stats = ""
    action_items_raw = ""
    claims = ""

    m = re.search(r"## Latest Stats\n(.*?)(?=\n---|\n## )", raw, re.DOTALL)
    if m:
        latest_stats = m.group(1).strip()

    m = re.search(r"## Action Items.*?\n(.*?)$", raw, re.DOTALL)
    if m:
        action_items_raw = m.group(1).strip()

    m = re.search(r"## Claims.*?\n(.*?)(?=\n## |$)", raw, re.DOTALL)
    if m:
        claims = m.group(1).strip()

    return {
        "latest_stats": latest_stats,
        "action_items": _parse_action_items(action_items_raw),
        "claims": claims,
    }


def _parse_agent_sections(raw: str) -> dict[str, str]:
    """Extract per-agent sections from bulletin markdown."""
    sections: dict[str, str] = {}
    for name in AGENT_NAMES:
        pattern = rf"## {re.escape(name)}\n(.*?)(?=\n---|\n## |$)"
        m = re.search(pattern, raw, re.DOTALL)
        sections[name] = m.group(1).strip() if m else ""
    return sections


def _get_recent_output_files(n: int = 4) -> list[Path]:
    """Get the n most recently modified .output files."""
    try:
        outputs = sorted(
            TASKS_DIR.glob("*.output"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        return outputs[:n]
    except (OSError, IOError):
        return []


def _guess_agent_name(log_tail: list[str], index: int) -> str:
    """Try to identify the agent from log content, fall back to positional name."""
    joined = "\n".join(log_tail).lower()
    for name in AGENT_NAMES:
        if name.lower() in joined:
            return name
    if index < len(AGENT_NAMES):
        return AGENT_NAMES[index]
    return f"Agent {index + 1}"


def _count_rate_limits(log_lines: list[str]) -> int:
    """Count rate-limit-related lines in the pipeline log."""
    count = 0
    for line in log_lines:
        lower = line.lower()
        if "rate limit" in lower or "429" in lower or "too many requests" in lower:
            count += 1
    return count


def _get_recent_chat_messages(limit: int = 30) -> list[dict]:
    """Fetch recent chat messages from the database."""
    try:
        from sqlalchemy import text
        s = _get_db_session()
        try:
            result = s.execute(
                text(
                    "SELECT id, message, context, page_url, ticker, created_at, addressed, response_thread "
                    "FROM chat_messages ORDER BY created_at DESC LIMIT :limit"
                ),
                {"limit": limit},
            )
            rows = result.mappings().fetchall()
            messages = []
            for r in rows:
                import json as _json
                ctx = r["context"]
                if isinstance(ctx, str):
                    try:
                        ctx = _json.loads(ctx)
                    except Exception:
                        ctx = {}
                messages.append({
                    "id": r["id"],
                    "message": r["message"],
                    "context": ctx or {},
                    "page_url": r["page_url"],
                    "ticker": r["ticker"],
                    "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                    "addressed": bool(r["addressed"]),
                    "response_thread": r.get("response_thread") or [],
                })
            messages.reverse()  # chronological order
            return messages
        finally:
            s.close()
    except Exception:
        return []


def _count_contributions(log_lines: list[str], bulletin_raw: str, agent_name: str) -> dict:
    """Count contributions for an agent from its log lines and bulletin mentions."""
    # Count completed cycles from log — match "Cycle N" patterns
    cycle_nums: set[str] = set()
    for l in log_lines:
        m = re.search(r"Cycle\s+(\d+)", l)
        if m:
            cycle_nums.add(m.group(1))
    cycles = len(cycle_nums)

    # Count fixes from bulletin RELEASED entries mentioning this agent or key
    fixes = 0
    name_lower = agent_name.lower()
    for line in bulletin_raw.splitlines():
        if "RELEASED:" in line and "FIXED" in line:
            if name_lower in line.lower():
                fixes += 1
    # Also count for short names
    short_names = {
        "Data Quality Guardian": ["guardian"],
        "Pipeline Optimizer": ["pipeline optimizer", "pipeline"],
        "Bottleneck Researcher": ["researcher", "sre"],
        "Code Quality Architect": ["architect"],
    }
    for alias in short_names.get(agent_name, []):
        for line in bulletin_raw.splitlines():
            if "RELEASED:" in line and "FIXED" in line:
                if alias in line.lower() and name_lower not in line.lower():
                    fixes += 1

    return {"cycles": cycles, "fixes": fixes, "total": cycles + fixes}


def build_agents_payload() -> dict:
    """Build the full agents payload.

    Returns the shape expected by the frontend AgentsPayload type:
      bulletin: {latest_stats, action_items, claims}
      agents: Record<string, AgentInfo>  (keyed by snake_case id)
      pipeline_log: string[]
      researcher_findings: string
      rate_limit_count: number
    """
    # 1. Bulletin board
    raw_bulletin = _read_file(BULLETIN_PATH)
    bulletin = _parse_bulletin(raw_bulletin)
    agent_sections = _parse_agent_sections(raw_bulletin)

    # 2. Build agents dict — prefer persistent script logs over stale task outputs
    agents: dict[str, dict] = {}

    # 2a. Populate from persistent monitor scripts (authoritative source)
    for key, pinfo in PERSISTENT_AGENTS.items():
        log_file = pinfo["log_file"]
        name = pinfo["name"]
        defn = AGENT_DEFS.get(name, {})
        log_lines = _tail_lines(log_file, 100)
        last_active = _file_mtime_iso(log_file)

        # If no log file, fall back to bulletin mtime
        if not last_active and BULLETIN_PATH.exists():
            last_active = _file_mtime_iso(BULLETIN_PATH)

        # Check if process is actually running via pgrep + log recency fallback
        status = "stopped"
        try:
            result = subprocess.run(
                ["/usr/bin/pgrep", "-f", pinfo["pgrep_pattern"]],
                capture_output=True, text=True, timeout=3,
            )
            if result.stdout.strip():
                status = "running"
        except Exception:
            pass

        # Fallback: if log file was updated recently, treat as running
        if status == "stopped" and last_active:
            try:
                mtime = log_file.stat().st_mtime
                age_seconds = datetime.now(tz=timezone.utc).timestamp() - mtime
                if age_seconds < 300:  # updated within 5 minutes
                    status = "running"
            except OSError:
                pass

        # If no log file but process is running, show bulletin section as log
        if not log_lines and status == "running":
            section = agent_sections.get(name, "")
            if section:
                log_lines = section.splitlines()

        agents[key] = {
            "name": name,
            "role": defn.get("role", "Agent"),
            "bulletin": agent_sections.get(name, ""),
            "log": log_lines,
            "last_active": last_active,
            "status": status,
        }

    # 2b. Fill remaining agents from task output files (legacy/fallback)
    output_files = _get_recent_output_files(4)
    used_names: set[str] = set(pa["name"] for pa in PERSISTENT_AGENTS.values())

    for i, path in enumerate(output_files):
        log_lines = _tail_lines(path, 100)
        name = _guess_agent_name(log_lines, i)
        if name in used_names:
            continue  # Already populated from persistent script
        used_names.add(name)

        defn = AGENT_DEFS.get(name, {})
        key = defn.get("key", name.lower().replace(" ", "_"))
        if key in agents:
            continue  # Already populated

        agents[key] = {
            "name": name,
            "role": defn.get("role", "Agent"),
            "bulletin": agent_sections.get(name, ""),
            "log": log_lines,
            "last_active": _file_mtime_iso(path),
            "status": "unknown",
        }

    # 2c. Fill in agents that have bulletin sections but no log source
    for name in AGENT_NAMES:
        defn = AGENT_DEFS[name]
        key = defn["key"]
        if key not in agents:
            agents[key] = {
                "name": name,
                "role": defn["role"],
                "bulletin": agent_sections.get(name, ""),
                "log": [],
                "last_active": None,
                "status": "idle",
            }

    # 2d. Add contribution counts to each agent
    for key, agent in agents.items():
        agent["contributions"] = _count_contributions(
            agent.get("log", []), raw_bulletin, agent["name"]
        )

    # 3. Pipeline log (last 50 lines as a list)
    pipeline_log = _tail_lines(PIPELINE_LOG, 50)

    # 4. Researcher findings
    researcher_findings = _read_file(RESEARCHER_FINDINGS)

    # 5. Rate limit count from pipeline log
    rate_limit_count = _count_rate_limits(pipeline_log)

    # 6. Recent chat messages
    chat_messages = _get_recent_chat_messages(30)

    return {
        "bulletin": bulletin,
        "agents": agents,
        "pipeline_log": pipeline_log,
        "researcher_findings": researcher_findings,
        "rate_limit_count": rate_limit_count,
        "chat_messages": chat_messages,
    }


@get("/api/agents")
async def get_agents_status() -> dict:
    """Litestar handler — delegates to build_agents_payload."""
    return build_agents_payload()


@post("/api/agents/bulletin")
async def post_bulletin_action(request: Request) -> dict:
    """Add an action item to the bulletin board."""
    body = await request.json()
    message = body.get("message", "").strip()

    if not message:
        return {"ok": False, "error": "message is required"}

    try:
        raw = _read_file(BULLETIN_PATH)
        if not raw:
            return {"ok": False, "error": "bulletin board not found"}

        marker = "## Action Items"
        idx = raw.find(marker)
        if idx == -1:
            raw += f"\n\n{marker} (cross-agent)\n{message}\n"
        else:
            raw = raw.rstrip() + f"\n\nFROM DASHBOARD: {message}\n"

        BULLETIN_PATH.write_text(raw)
        return {"ok": True}
    except (OSError, IOError) as e:
        return {"ok": False, "error": str(e)}


@post("/api/agents/dismiss-resolved")
async def dismiss_resolved_items(request: Request) -> dict:
    """Remove all resolved action items from the bulletin board.

    If the request body contains {"summary": "..."}, dismiss only that item.
    Otherwise dismiss all resolved items.
    """
    body = await request.json() if request.content_type and "json" in request.content_type else {}
    target_summary = body.get("summary", "").strip() if body else ""

    try:
        raw = _read_file(BULLETIN_PATH)
        if not raw:
            return {"ok": False, "error": "bulletin board not found"}

        # Find the action items section
        ai_match = re.search(r"## Action Items.*?\n", raw)
        if not ai_match:
            return {"ok": True, "dismissed": 0}

        before = raw[:ai_match.end()]
        after = raw[ai_match.end():]

        # Parse items from the action items section
        item_re = re.compile(r"^(FOR\s+.+?:|FROM\s+DASHBOARD:)", re.MULTILINE)
        splits = list(item_re.finditer(after))
        if not splits:
            return {"ok": True, "dismissed": 0}

        resolved_markers = ("**ANSWERED", "**DONE", "**FIXED", "**INVESTIGATED")
        kept_parts: list[str] = []
        dismissed = 0

        # Text before first item (preamble)
        preamble = after[:splits[0].start()] if splits else after
        kept_parts.append(preamble)

        for idx, m in enumerate(splits):
            start = m.start()
            end = splits[idx + 1].start() if idx + 1 < len(splits) else len(after)
            chunk = after[start:end]

            is_resolved = any(mk in chunk for mk in resolved_markers)

            if is_resolved:
                if target_summary:
                    # Only dismiss the specific item matching the summary
                    clean_chunk = chunk.replace("**", "").strip()
                    if target_summary.lower() in clean_chunk.lower():
                        dismissed += 1
                        continue
                    else:
                        kept_parts.append(chunk)
                else:
                    dismissed += 1
                    continue
            else:
                kept_parts.append(chunk)

        new_raw = before + "".join(kept_parts)
        BULLETIN_PATH.write_text(new_raw)
        return {"ok": True, "dismissed": dismissed}

    except (OSError, IOError) as e:
        return {"ok": False, "error": str(e)}


# ── Team-based Agent API (DB-backed) ──────────────────────────────────


@get("/api/agents/teams")
async def get_agent_teams() -> list[dict]:
    """List all teams with agent roster and stats."""
    session = _get_db_session()
    try:
        teams = session.execute(sa_text("""
            SELECT t.id, t.name, t.display_name, t.description
            FROM agent_teams t ORDER BY t.id
        """)).mappings().fetchall()

        result = []
        for team in teams:
            agents = session.execute(sa_text("""
                SELECT a.id, a.name, a.role, a.display_name, a.status, a.pid,
                       a.last_heartbeat, a.cycle_count, a.fix_count,
                       COALESCE(mc.cnt, 0) as message_count,
                       mc.last_msg as last_message_at
                FROM agents a
                LEFT JOIN (
                    SELECT agent_id, COUNT(*) as cnt, MAX(created_at) as last_msg
                    FROM agent_messages GROUP BY agent_id
                ) mc ON mc.agent_id = a.id
                WHERE a.team_id = :team_id ORDER BY a.role
            """), {"team_id": team["id"]}).mappings().fetchall()

            # Inline metrics for the team
            plan_metrics = session.execute(sa_text("""
                SELECT
                    COUNT(*) FILTER (WHERE status = 'draft' OR status = 'review') as plans_pending,
                    COUNT(*) FILTER (WHERE status = 'approved' OR status = 'implementing') as plans_active,
                    COUNT(*) FILTER (WHERE status = 'verified') as plans_verified,
                    COUNT(*) FILTER (WHERE status = 'rejected') as plans_rejected
                FROM agent_plans WHERE team_id = :team_id
            """), {"team_id": team["id"]}).mappings().fetchone()

            proj_metrics = session.execute(sa_text("""
                SELECT
                    COUNT(*) as projects_total,
                    COUNT(*) FILTER (WHERE status = 'verified') as projects_verified,
                    COUNT(*) FILTER (WHERE status = 'failed') as projects_failed,
                    COUNT(pr_number) as prs_total,
                    COUNT(*) FILTER (WHERE human_review_status = 'pass') as human_reviews_pass,
                    COUNT(*) FILTER (WHERE human_review_status = 'fail') as human_reviews_fail
                FROM agent_projects WHERE team_id = :team_id
            """), {"team_id": team["id"]}).mappings().fetchone()

            result.append({
                **dict(team),
                "agents": [dict(a) for a in agents],
                "metrics": {
                    "plans_pending": plan_metrics["plans_pending"] if plan_metrics else 0,
                    "plans_active": plan_metrics["plans_active"] if plan_metrics else 0,
                    "plans_verified": plan_metrics["plans_verified"] if plan_metrics else 0,
                    "plans_rejected": plan_metrics["plans_rejected"] if plan_metrics else 0,
                    "projects_total": proj_metrics["projects_total"] if proj_metrics else 0,
                    "projects_verified": proj_metrics["projects_verified"] if proj_metrics else 0,
                    "projects_failed": proj_metrics["projects_failed"] if proj_metrics else 0,
                    "prs_total": proj_metrics["prs_total"] if proj_metrics else 0,
                    "human_reviews_pass": proj_metrics["human_reviews_pass"] if proj_metrics else 0,
                    "human_reviews_fail": proj_metrics["human_reviews_fail"] if proj_metrics else 0,
                },
            })

        # Add supervisor (no team)
        supervisor = session.execute(sa_text("""
            SELECT a.id, a.name, a.role, a.display_name, a.status, a.pid,
                   a.last_heartbeat, a.cycle_count, a.fix_count,
                   COALESCE(mc.cnt, 0) as message_count,
                   mc.last_msg as last_message_at
            FROM agents a
            LEFT JOIN (
                SELECT agent_id, COUNT(*) as cnt, MAX(created_at) as last_msg
                FROM agent_messages GROUP BY agent_id
            ) mc ON mc.agent_id = a.id
            WHERE a.team_id IS NULL AND a.role = 'supervisor'
        """)).mappings().fetchall()

        if supervisor:
            result.insert(0, {
                "id": None,
                "name": "supervisor",
                "display_name": "Supervisor",
                "description": "Oversees all teams and resolves conflicts",
                "agents": [dict(s) for s in supervisor],
            })

        return result
    finally:
        session.close()


@get("/api/agents/teams/{team_id:int}")
async def get_agent_team_detail(team_id: int) -> dict:
    """Single team detail."""
    session = _get_db_session()
    try:
        team = session.execute(sa_text("""
            SELECT id, name, display_name, description
            FROM agent_teams WHERE id = :id
        """), {"id": team_id}).mappings().fetchone()

        if not team:
            return {"error": "Team not found"}

        agents = session.execute(sa_text("""
            SELECT id, name, role, display_name, status, pid,
                   last_heartbeat, cycle_count, fix_count
            FROM agents WHERE team_id = :team_id ORDER BY role
        """), {"team_id": team_id}).mappings().fetchall()

        return {**dict(team), "agents": [dict(a) for a in agents]}
    finally:
        session.close()


@get("/api/agents/channels")
async def get_agent_channels() -> list[dict]:
    """List all channels."""
    session = _get_db_session()
    try:
        channels = session.execute(sa_text("""
            SELECT c.id, c.name, c.channel_type, c.description,
                   (SELECT COUNT(*) FROM agent_messages m WHERE m.channel_id = c.id) as message_count
            FROM agent_channels c ORDER BY c.name
        """)).mappings().fetchall()
        return [dict(c) for c in channels]
    finally:
        session.close()


@get("/api/agents/channels/{channel_name:str}/messages")
async def get_channel_messages(channel_name: str, since: str | None = None, limit: int = 50) -> list[dict]:
    """Get paginated messages for a channel."""
    # Prepend # if not present
    if not channel_name.startswith("#"):
        channel_name = f"#{channel_name}"

    session = _get_db_session()
    try:
        params: dict = {"channel": channel_name, "limit": min(limit, 200)}

        if since:
            query = """
                SELECT m.id, m.sender_name, m.content, m.message_type, m.metadata, m.created_at,
                       a.role as sender_role, a.display_name as sender_display_name
                FROM agent_messages m
                LEFT JOIN agents a ON a.id = m.agent_id
                JOIN agent_channels c ON c.id = m.channel_id
                WHERE c.name = :channel AND m.created_at > :since
                ORDER BY m.created_at ASC LIMIT :limit
            """
            params["since"] = since
        else:
            query = """
                SELECT m.id, m.sender_name, m.content, m.message_type, m.metadata, m.created_at,
                       a.role as sender_role, a.display_name as sender_display_name
                FROM agent_messages m
                LEFT JOIN agents a ON a.id = m.agent_id
                JOIN agent_channels c ON c.id = m.channel_id
                WHERE c.name = :channel
                ORDER BY m.created_at DESC LIMIT :limit
            """

        rows = session.execute(sa_text(query), params).mappings().fetchall()
        messages = [dict(r) for r in rows]

        # If we queried DESC (no since), reverse to chronological
        if not since:
            messages.reverse()

        return messages
    finally:
        session.close()


@post("/api/agents/channels/{channel_name:str}/messages")
async def post_channel_message(channel_name: str, data: dict) -> dict:
    """Human posts a message to a channel."""
    if not channel_name.startswith("#"):
        channel_name = f"#{channel_name}"

    content = data.get("content", "").strip()
    sender = data.get("sender_name", "human")

    if not content:
        return {"ok": False, "error": "content is required"}

    session = _get_db_session()
    try:
        session.execute(sa_text("""
            INSERT INTO agent_messages (channel_id, agent_id, sender_name, content, message_type, created_at)
            SELECT c.id, NULL, :sender, :content, 'human_input', NOW()
            FROM agent_channels c WHERE c.name = :channel
        """), {"sender": sender, "content": content, "channel": channel_name})
        session.commit()
        return {"ok": True}
    except Exception as e:
        session.rollback()
        return {"ok": False, "error": str(e)}
    finally:
        session.close()


@get("/api/agents/plans")
async def get_agent_plans(team_id: int | None = None, status: str | None = None) -> list[dict]:
    """List plans with optional filters."""
    session = _get_db_session()
    try:
        conditions = []
        params: dict = {}

        if team_id is not None:
            conditions.append("p.team_id = :team_id")
            params["team_id"] = team_id
        if status:
            conditions.append("p.status = :status")
            params["status"] = status

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        rows = session.execute(sa_text(f"""
            SELECT p.id, p.team_id, p.title, p.description, p.status,
                   p.created_at, p.updated_at,
                   p.pr_number, p.pr_url, p.pr_branch,
                   t.display_name as team_name,
                   ca.display_name as created_by_name,
                   ra.display_name as reviewed_by_name,
                   (SELECT COUNT(*) FROM agent_plan_comments c WHERE c.plan_id = p.id) as comment_count
            FROM agent_plans p
            JOIN agent_teams t ON t.id = p.team_id
            LEFT JOIN agents ca ON ca.id = p.created_by
            LEFT JOIN agents ra ON ra.id = p.reviewed_by
            {where}
            ORDER BY p.created_at DESC
        """), params).mappings().fetchall()

        return [dict(r) for r in rows]
    finally:
        session.close()


@get("/api/agents/plans/{plan_id:int}")
async def get_agent_plan_detail(plan_id: int) -> dict:
    """Plan detail with comments."""
    session = _get_db_session()
    try:
        plan = session.execute(sa_text("""
            SELECT p.*, t.display_name as team_name,
                   ca.display_name as created_by_name,
                   ra.display_name as reviewed_by_name
            FROM agent_plans p
            JOIN agent_teams t ON t.id = p.team_id
            LEFT JOIN agents ca ON ca.id = p.created_by
            LEFT JOIN agents ra ON ra.id = p.reviewed_by
            WHERE p.id = :id
        """), {"id": plan_id}).mappings().fetchone()

        if not plan:
            return {"error": "Plan not found"}

        comments = session.execute(sa_text("""
            SELECT id, plan_id, line_number, author_name, content, resolved, created_at
            FROM agent_plan_comments
            WHERE plan_id = :plan_id
            ORDER BY created_at ASC
        """), {"plan_id": plan_id}).mappings().fetchall()

        return {**dict(plan), "comments": [dict(c) for c in comments]}
    finally:
        session.close()


@post("/api/agents/plans/{plan_id:int}/comments")
async def post_plan_comment(plan_id: int, data: dict) -> dict:
    """Add a line-level comment to a plan."""
    content = data.get("content", "").strip()
    author = data.get("author_name", "human")
    line_number = data.get("line_number")  # None for general comment

    if not content:
        return {"ok": False, "error": "content is required"}

    session = _get_db_session()
    try:
        session.execute(sa_text("""
            INSERT INTO agent_plan_comments (plan_id, line_number, author_name, content, created_at)
            VALUES (:plan_id, :line_number, :author, :content, NOW())
        """), {"plan_id": plan_id, "line_number": line_number, "author": author, "content": content})
        session.commit()
        return {"ok": True}
    except Exception as e:
        session.rollback()
        return {"ok": False, "error": str(e)}
    finally:
        session.close()


@put("/api/agents/plans/{plan_id:int}/status")
async def update_plan_status(plan_id: int, data: dict) -> dict:
    """Human approves/rejects a plan. Posts notification to the relevant team channel."""
    new_status = data.get("status", "").strip()

    valid = {"draft", "review", "approved", "implementing", "implemented", "verified", "rejected"}
    if new_status not in valid:
        return {"ok": False, "error": f"Invalid status. Must be one of: {', '.join(sorted(valid))}"}

    session = _get_db_session()
    try:
        # Get plan info before updating
        plan_row = session.execute(sa_text("""
            SELECT p.title, p.pr_number, t.name as team_name
            FROM agent_plans p JOIN agent_teams t ON t.id = p.team_id
            WHERE p.id = :id
        """), {"id": plan_id}).mappings().fetchone()

        if not plan_row:
            return {"ok": False, "error": "Plan not found"}

        team_name = plan_row["team_name"]
        plan_title = plan_row["title"]
        pr_number = plan_row["pr_number"]
        channel = f"#{team_name}"

        # If approving and there's a PR, merge it into main
        pr_merged = False
        if new_status == "approved" and pr_number:
            pr_merged = _merge_pr(pr_number)
            if not pr_merged:
                logger.warning("Failed to merge PR #%d for plan %d, approving anyway", pr_number, plan_id)

        session.execute(sa_text("""
            UPDATE agent_plans SET status = :status, updated_at = NOW() WHERE id = :id
        """), {"status": new_status, "id": plan_id})

        # Post notification to the team channel so agents react
        if new_status == "approved":
            merge_note = f" PR #{pr_number} merged into main." if pr_merged else ""
            msg = f'[Human Review] Plan "{plan_title}" has been APPROVED.{merge_note} Engineer, please pick this up for implementation.'
        elif new_status == "rejected":
            msg = f'[Human Review] Plan "{plan_title}" has been REJECTED. Idea guy, please revise or create a new plan.'
        else:
            msg = f'[Human Review] Plan "{plan_title}" status changed to {new_status}.'

        session.execute(sa_text("""
            INSERT INTO agent_messages (channel_id, agent_id, sender_name, content, message_type, created_at)
            SELECT c.id, NULL, 'human', :content, 'system', NOW()
            FROM agent_channels c WHERE c.name = :channel
        """), {"content": msg, "channel": channel})

        session.commit()
        return {"ok": True, "team_name": team_name, "status": new_status, "pr_merged": pr_merged}
    except Exception as e:
        session.rollback()
        return {"ok": False, "error": str(e)}
    finally:
        session.close()


@get("/api/agents/projects")
async def get_agent_projects() -> list[dict]:
    """List projects with status."""
    session = _get_db_session()
    try:
        rows = session.execute(sa_text("""
            SELECT p.id, p.plan_id, p.team_id, p.title, p.status,
                   p.files_changed, p.test_results, p.created_at, p.completed_at,
                   p.pr_number, p.pr_url, p.code_impact,
                   p.test_instructions, p.human_review_status, p.human_review_notes,
                   t.display_name as team_name,
                   ea.display_name as assigned_to_name,
                   ra.display_name as reviewer_name
            FROM agent_projects p
            JOIN agent_teams t ON t.id = p.team_id
            LEFT JOIN agents ea ON ea.id = p.assigned_to
            LEFT JOIN agents ra ON ra.id = p.reviewer_id
            ORDER BY p.created_at DESC
        """)).mappings().fetchall()
        return [dict(r) for r in rows]
    finally:
        session.close()


@get("/api/agents/projects/{project_id:int}")
async def get_agent_project_detail(project_id: int) -> dict:
    """Project detail."""
    session = _get_db_session()
    try:
        row = session.execute(sa_text("""
            SELECT p.*, t.display_name as team_name,
                   ea.display_name as assigned_to_name,
                   ra.display_name as reviewer_name
            FROM agent_projects p
            JOIN agent_teams t ON t.id = p.team_id
            LEFT JOIN agents ea ON ea.id = p.assigned_to
            LEFT JOIN agents ra ON ra.id = p.reviewer_id
            WHERE p.id = :id
        """), {"id": project_id}).mappings().fetchone()

        if not row:
            return {"error": "Project not found"}

        return dict(row)
    finally:
        session.close()


def _compute_team_metrics(session, team_id: int) -> dict:
    """Compute metrics for a single team from plans and projects tables."""
    plan_row = session.execute(sa_text("""
        SELECT
            COUNT(*) as plans_created,
            COUNT(*) FILTER (WHERE status = 'approved') as plans_approved,
            COUNT(*) FILTER (WHERE status = 'rejected') as plans_rejected,
            COUNT(*) FILTER (WHERE status = 'verified') as plans_verified,
            COUNT(*) FILTER (WHERE status = 'implementing') as plans_in_progress
        FROM agent_plans WHERE team_id = :team_id
    """), {"team_id": team_id}).mappings().fetchone()

    proj_row = session.execute(sa_text("""
        SELECT
            COUNT(*) as projects_total,
            COUNT(*) FILTER (WHERE status = 'verified') as projects_verified,
            COUNT(*) FILTER (WHERE status = 'failed') as projects_failed,
            COUNT(pr_number) as prs_open,
            COUNT(*) FILTER (WHERE pr_number IS NOT NULL AND status = 'verified') as prs_merged,
            COUNT(*) FILTER (WHERE human_review_status = 'pass') as human_reviews_pass,
            COUNT(*) FILTER (WHERE human_review_status = 'fail') as human_reviews_fail
        FROM agent_projects WHERE team_id = :team_id
    """), {"team_id": team_id}).mappings().fetchone()

    return {
        "plans_created": plan_row["plans_created"] if plan_row else 0,
        "plans_approved": plan_row["plans_approved"] if plan_row else 0,
        "plans_rejected": plan_row["plans_rejected"] if plan_row else 0,
        "plans_verified": plan_row["plans_verified"] if plan_row else 0,
        "plans_in_progress": plan_row["plans_in_progress"] if plan_row else 0,
        "projects_total": proj_row["projects_total"] if proj_row else 0,
        "projects_verified": proj_row["projects_verified"] if proj_row else 0,
        "projects_failed": proj_row["projects_failed"] if proj_row else 0,
        "prs_open": proj_row["prs_open"] if proj_row else 0,
        "prs_merged": proj_row["prs_merged"] if proj_row else 0,
        "human_reviews_pass": proj_row["human_reviews_pass"] if proj_row else 0,
        "human_reviews_fail": proj_row["human_reviews_fail"] if proj_row else 0,
    }


@get("/api/agents/teams/{team_id:int}/metrics")
async def get_team_metrics(team_id: int) -> dict:
    """Computed metrics for a single team."""
    session = _get_db_session()
    try:
        # Verify team exists
        team = session.execute(sa_text(
            "SELECT id FROM agent_teams WHERE id = :id"
        ), {"id": team_id}).fetchone()

        if not team:
            return {"error": "Team not found"}

        return _compute_team_metrics(session, team_id)
    finally:
        session.close()


@get("/api/agents/metrics/summary")
async def get_all_teams_metrics() -> list[dict]:
    """Metrics for all teams at once (avoids N+1 on the roster page)."""
    session = _get_db_session()
    try:
        teams = session.execute(sa_text(
            "SELECT id, name, display_name FROM agent_teams ORDER BY id"
        )).mappings().fetchall()

        result = []
        for team in teams:
            metrics = _compute_team_metrics(session, team["id"])
            result.append({
                "team_id": team["id"],
                "team_name": team["name"],
                "display_name": team["display_name"],
                **metrics,
            })

        return result
    finally:
        session.close()


@post("/api/agents/projects/{project_id:int}/review")
async def post_project_review(project_id: int, data: dict) -> dict:
    """Human submits a pass/fail review for a project."""
    review_status = data.get("status", "").strip()
    notes = data.get("notes", "").strip()

    if review_status not in ("pass", "fail"):
        return {"ok": False, "error": "status must be 'pass' or 'fail'"}

    session = _get_db_session()
    try:
        # Get project info
        proj = session.execute(sa_text("""
            SELECT p.id, p.title, p.team_id, t.name as team_name
            FROM agent_projects p
            JOIN agent_teams t ON t.id = p.team_id
            WHERE p.id = :id
        """), {"id": project_id}).mappings().fetchone()

        if not proj:
            return {"ok": False, "error": "Project not found"}

        # Update project review fields
        if review_status == "fail":
            session.execute(sa_text("""
                UPDATE agent_projects
                SET human_review_status = :review_status,
                    human_review_notes = :notes,
                    status = 'failed'
                WHERE id = :id
            """), {"review_status": review_status, "notes": notes, "id": project_id})
        else:
            session.execute(sa_text("""
                UPDATE agent_projects
                SET human_review_status = :review_status,
                    human_review_notes = :notes
                WHERE id = :id
            """), {"review_status": review_status, "notes": notes, "id": project_id})

        # Post notification to team channel
        channel = f"#{proj['team_name']}"
        title = proj["title"]

        if review_status == "fail":
            msg = f'[Human Review] Project "{title}" \u2014 testing FAILED. Notes: {notes}. Idea guy, please investigate.'
        else:
            msg = f'[Human Review] Project "{title}" \u2014 testing PASSED. Implementation verified by human.'

        session.execute(sa_text("""
            INSERT INTO agent_messages (channel_id, agent_id, sender_name, content, message_type, created_at)
            SELECT c.id, NULL, 'human', :content, 'system', NOW()
            FROM agent_channels c WHERE c.name = :channel
        """), {"content": msg, "channel": channel})

        session.commit()
        return {"ok": True, "status": review_status, "project_id": project_id}
    except Exception as e:
        session.rollback()
        return {"ok": False, "error": str(e)}
    finally:
        session.close()
