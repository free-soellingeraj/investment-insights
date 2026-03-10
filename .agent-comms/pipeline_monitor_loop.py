#!/usr/bin/env python3
"""Persistent pipeline health monitor — runs indefinitely, cycles every 90s."""

import json
import os
import re
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2

DB_DSN = f"postgresql://{os.environ.get('USER', 'postgres')}:@localhost:5432/ai_opportunity_index"
BULLETIN = Path(__file__).resolve().parent / "bulletin.md"
PIPELINE_LOG = Path("/tmp/parallel_pipeline.log")
LOG_FILE = Path("/tmp/pipeline_monitor_loop.log")
CYCLE_SLEEP = 90


def log(msg: str):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


def db_query(query: str, params=None):
    conn = psycopg2.connect(DB_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            if cur.description:
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
            conn.commit()
            return cur.rowcount
    finally:
        conn.close()


def db_scalar(query: str, params=None):
    rows = db_query(query, params)
    if rows and isinstance(rows, list):
        return list(rows[0].values())[0]
    return None


def count_pipeline_processes():
    try:
        result = subprocess.run(
            ["pgrep", "-f", "run_pipeline"],
            capture_output=True, text=True
        )
        return len(result.stdout.strip().splitlines()) if result.stdout.strip() else 0
    except Exception:
        return -1


def check_log_errors(n=100):
    if not PIPELINE_LOG.exists():
        return 0, 0, []
    try:
        with open(PIPELINE_LOG, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            chunk = min(size, 65536)
            f.seek(-chunk, 2)
            data = f.read().decode("utf-8", errors="replace")
        lines = data.splitlines()[-n:]
        errors = [l for l in lines if re.search(r'error|exception|traceback', l, re.I)]
        rate_limits = sum(1 for l in lines if '429' in l or 'RESOURCE_EXHAUSTED' in l)
        return len(errors), rate_limits, errors[-5:]
    except Exception:
        return 0, 0, []


def check_zombie_runs():
    return db_scalar(
        "SELECT COUNT(*) FROM pipeline_runs WHERE status='running' AND started_at < NOW() - interval '2 hours'"
    ) or 0


def fix_zombie_runs():
    return db_query(
        "UPDATE pipeline_runs SET status='failed', completed_at=NOW() "
        "WHERE status='running' AND started_at < NOW() - interval '2 hours'"
    )


def get_db_stats():
    return {
        "companies": db_scalar("SELECT COUNT(*) FROM companies"),
        "scored": db_scalar("SELECT COUNT(DISTINCT company_id) FROM company_scores"),
        "evidence_groups": db_scalar("SELECT COUNT(*) FROM evidence_groups"),
        "valuations": db_scalar("SELECT COUNT(*) FROM valuations"),
        "projects": db_scalar("SELECT COUNT(*) FROM investment_projects"),
        "pipeline_runs": db_scalar("SELECT COUNT(*) FROM pipeline_runs"),
        "active_runs": db_scalar("SELECT COUNT(*) FROM pipeline_runs WHERE status='running'"),
    }


def update_bulletin(section_content: str):
    """Update the Pipeline Optimizer section of the bulletin."""
    if not BULLETIN.exists():
        return
    raw = BULLETIN.read_text(errors="replace")
    marker = "## Pipeline Optimizer"
    next_section = re.compile(r"\n---\n|\n## (?!Pipeline Optimizer)")

    idx = raw.find(marker)
    if idx == -1:
        # Add section before Action Items or at end
        ai_idx = raw.find("## Action Items")
        if ai_idx != -1:
            raw = raw[:ai_idx] + f"{marker}\n{section_content}\n\n---\n\n" + raw[ai_idx:]
        else:
            raw += f"\n\n{marker}\n{section_content}\n"
    else:
        # Replace existing section content
        content_start = raw.index("\n", idx) + 1
        m = next_section.search(raw, content_start)
        content_end = m.start() if m else len(raw)
        raw = raw[:content_start] + section_content + "\n" + raw[content_end:]

    BULLETIN.write_text(raw)


def run_cycle(cycle_num: int):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    log(f"=== Cycle {cycle_num} ===")

    # 1. Pipeline processes
    proc_count = count_pipeline_processes()
    log(f"Pipeline processes: {proc_count}")

    # 2. Log errors
    error_count, rate_limit_count, recent_errors = check_log_errors()
    log(f"Log errors: {error_count}, rate limits: {rate_limit_count}")

    # 3. DB stats
    stats = get_db_stats()
    log(f"DB: {stats}")

    # 4. Zombie runs
    zombies = check_zombie_runs()
    fixed = 0
    if zombies > 0:
        fixed = fix_zombie_runs()
        log(f"Fixed {fixed} zombie pipeline runs")

    # 5. Build bulletin section
    lines = [
        f"_Last update: {ts} — Cycle {cycle_num}_",
        "",
        f"**Pipeline Processes:** {proc_count} running",
        f"**DB Stats:** {stats['companies']} companies, {stats['scored']} scored, "
        f"{stats['evidence_groups']} evidence groups, {stats['valuations']} valuations, "
        f"{stats['projects']} projects",
        f"**Pipeline Runs:** {stats['pipeline_runs']} total, {stats['active_runs']} active",
        f"**Rate Limits (last 100 lines):** {rate_limit_count}",
    ]

    if zombies > 0:
        lines.append(f"**Zombie Runs Fixed:** {fixed}")

    if error_count > 0:
        lines.append(f"**Recent Errors:** {error_count}")
        for e in recent_errors:
            lines.append(f"  - {e[:120]}")

    if proc_count == 0:
        lines.append("")
        lines.append("**WARNING: No pipeline processes running!**")

    update_bulletin("\n".join(lines))
    log(f"Cycle {cycle_num} complete")


def main():
    log("Pipeline monitor starting (persistent loop)")
    cycle = 0
    while True:
        cycle += 1
        try:
            run_cycle(cycle)
        except Exception as e:
            log(f"Cycle {cycle} error: {e}")
        time.sleep(CYCLE_SLEEP)


if __name__ == "__main__":
    main()
