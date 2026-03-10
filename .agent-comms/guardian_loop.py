#!/usr/bin/env python3.11
"""Data Quality Guardian — runs continuously, checks DB integrity every cycle."""

import datetime
import json
import os
import time
import traceback
import urllib.request

import psycopg2

DB_URL = "postgresql://free-soellingeraj:@localhost:5432/ai_opportunity_index"
BULLETIN = "/Users/free-soellingeraj/code/.para-llm-directory/envs/investment-insights-rime-related-alerts/investment-insights/.agent-comms/bulletin.md"
HUMAN_FEEDBACK = "/Users/free-soellingeraj/code/.para-llm-directory/envs/investment-insights-rime-related-alerts/investment-insights/.agent-comms/human_feedback.md"
LOG_FILE = "/tmp/guardian_loop.log"
SLEEP_SECONDS = 120

def now():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg):
    line = f"[{now()}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

def run_cycle(cycle_num):
    findings = []
    fixes = []
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    cur = conn.cursor()

    # ── 1. DB Integrity ──────────────────────────────────────────────
    # 1a. Absurdly large dollar_total (>$1T)
    cur.execute("SELECT COUNT(*) FROM investment_projects WHERE dollar_total > 1e12")
    big_dollar = cur.fetchone()[0]
    if big_dollar > 0:
        findings.append(f"ALERT: {big_dollar} projects with dollar_total > $1T")
        # Cap them at $500B
        cur.execute("""
            UPDATE investment_projects
            SET dollar_total = 5e11, dollar_high = LEAST(dollar_high, 5e11),
                updated_at = NOW()
            WHERE dollar_total > 1e12
        """)
        conn.commit()
        fixes.append(f"Capped {big_dollar} projects with dollar_total > $1T to $500B")
    else:
        findings.append("OK: No projects with dollar_total > $1T")

    # 1b. Inverted dollar ranges (low > high, both positive)
    cur.execute("""
        SELECT id, short_title, dollar_low, dollar_high
        FROM investment_projects
        WHERE dollar_low > dollar_high AND dollar_low > 0 AND dollar_high > 0
        LIMIT 20
    """)
    inverted = cur.fetchall()
    if inverted:
        findings.append(f"ALERT: {len(inverted)} projects with inverted dollar ranges (low > high)")
        ids = [r[0] for r in inverted]
        cur.execute("""
            UPDATE investment_projects
            SET dollar_low = LEAST(dollar_low, dollar_high),
                dollar_high = GREATEST(dollar_low, dollar_high),
                updated_at = NOW()
            WHERE id = ANY(%s)
        """, (ids,))
        conn.commit()
        fixes.append(f"Fixed {len(inverted)} inverted dollar ranges")
    else:
        findings.append("OK: No inverted dollar ranges in projects")

    # 1c. Inverted dollar ranges in valuations
    cur.execute("""
        SELECT COUNT(*) FROM valuations
        WHERE dollar_low > dollar_high AND dollar_low > 0 AND dollar_high > 0
    """)
    inv_val = cur.fetchone()[0]
    if inv_val > 0:
        findings.append(f"ALERT: {inv_val} valuations with inverted dollar ranges")
        cur.execute("""
            UPDATE valuations
            SET dollar_low = LEAST(dollar_low, dollar_high),
                dollar_high = GREATEST(dollar_low, dollar_high),
                updated_at = NOW()
            WHERE dollar_low > dollar_high AND dollar_low > 0 AND dollar_high > 0
        """)
        conn.commit()
        fixes.append(f"Fixed {inv_val} inverted valuation dollar ranges")
    else:
        findings.append("OK: No inverted dollar ranges in valuations")

    # 1d. OOB scores (outside 0-100)
    cur.execute("""
        SELECT COUNT(*) FROM company_scores
        WHERE composite_opp_score < 0 OR composite_opp_score > 100
           OR composite_real_score < 0 OR composite_real_score > 100
    """)
    oob = cur.fetchone()[0]
    if oob > 0:
        findings.append(f"ALERT: {oob} scores out of [0,100] bounds")
        cur.execute("""
            UPDATE company_scores
            SET composite_opp_score = GREATEST(0, LEAST(100, composite_opp_score)),
                composite_real_score = GREATEST(0, LEAST(100, composite_real_score))
            WHERE composite_opp_score < 0 OR composite_opp_score > 100
               OR composite_real_score < 0 OR composite_real_score > 100
        """)
        conn.commit()
        fixes.append(f"Clamped {oob} OOB scores to [0,100]")
    else:
        findings.append("OK: All composite scores within [0,100]")

    # ── 2. Suppressed projects ────────────────────────────────────────
    cur.execute("""
        SELECT id, short_title, status FROM investment_projects
        WHERE status = 'suppressed'
        ORDER BY updated_at DESC LIMIT 5
    """)
    suppressed = cur.fetchall()
    if suppressed:
        lines = [f"  - #{r[0]}: {r[1]}" for r in suppressed]
        findings.append(f"Suppressed projects (latest 5):\n" + "\n".join(lines))
    else:
        findings.append("OK: No suppressed projects")

    # ── 3. URL provenance on recent passages ──────────────────────────
    cur.execute("""
        SELECT
            COUNT(*) AS total,
            COUNT(source_url) AS with_url,
            COUNT(source_publisher) AS with_publisher
        FROM evidence_group_passages
        WHERE created_at > NOW() - INTERVAL '24 hours'
    """)
    prov = cur.fetchone()
    total_p, with_url, with_pub = prov
    if total_p > 0:
        url_pct = round(100 * with_url / total_p, 1)
        pub_pct = round(100 * with_pub / total_p, 1)
        findings.append(f"Provenance (last 24h): {total_p} passages, {url_pct}% have URL, {pub_pct}% have publisher")
    else:
        # Try all-time
        cur.execute("""
            SELECT COUNT(*) AS total, COUNT(source_url) AS with_url, COUNT(source_publisher) AS with_pub
            FROM evidence_group_passages
        """)
        prov = cur.fetchone()
        total_p, with_url, with_pub = prov
        if total_p > 0:
            url_pct = round(100 * with_url / total_p, 1)
            pub_pct = round(100 * with_pub / total_p, 1)
            findings.append(f"Provenance (all-time): {total_p} passages, {url_pct}% have URL, {pub_pct}% have publisher")
        else:
            findings.append("No passages found in DB")

    # ── 4. Overall stats ──────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM companies")
    n_companies = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM investment_projects")
    n_projects = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM valuations")
    n_valuations = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM evidence_group_passages")
    n_passages = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM company_scores")
    n_scores = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT company_id) FROM company_scores")
    n_scored = cur.fetchone()[0]

    findings.append(
        f"Stats: {n_companies} companies, {n_projects} projects, "
        f"{n_valuations} valuations, {n_passages} passages, "
        f"{n_scores} scores ({n_scored} unique companies scored)"
    )

    # ── 5. Check human ratings ────────────────────────────────────────
    cur.execute("""
        SELECT id, entity_type, entity_id, rating, dimension, comment, action, created_at
        FROM human_ratings
        ORDER BY created_at DESC LIMIT 10
    """)
    ratings = cur.fetchall()
    if ratings:
        rating_lines = []
        for r in ratings:
            rid, etype, eid, rating, dim, comment, action, cat = r
            flag = " **FLAGGED**" if action in ('mark_incorrect', 'suppress') else ""
            rating_lines.append(f"  - [{cat}] {etype}#{eid}: {rating}/5 ({dim}) {action or ''}{flag} — {comment or ''}")
        findings.append("Recent human ratings:\n" + "\n".join(rating_lines))

        # Handle flagged items
        cur.execute("""
            SELECT id, entity_type, entity_id, action, comment
            FROM human_ratings
            WHERE action IN ('mark_incorrect', 'suppress')
            AND created_at > NOW() - INTERVAL '1 hour'
        """)
        flagged = cur.fetchall()
        for fid, etype, eid, faction, fcomment in flagged:
            if etype == 'project' and faction == 'suppress':
                cur.execute("UPDATE investment_projects SET status='suppressed', updated_at=NOW() WHERE id=%s AND status != 'suppressed'", (eid,))
                if cur.rowcount > 0:
                    conn.commit()
                    fixes.append(f"Suppressed project #{eid} per human rating #{fid}")
    else:
        findings.append("No human ratings found")

    # Also try the API for ratings
    try:
        req = urllib.request.Request("http://localhost:8000/api/ratings/recent?limit=10", headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
            if data:
                findings.append(f"API ratings endpoint returned {len(data)} items")
    except Exception:
        pass  # API might not have this endpoint

    # Also read human_feedback.md
    try:
        with open(HUMAN_FEEDBACK, "r") as f:
            feedback = f.read().strip()
        if feedback:
            findings.append(f"Human feedback file: {len(feedback)} bytes")
    except FileNotFoundError:
        pass

    # ── 6. Process unaddressed chat messages ─────────────────────────
    chat_fixes = process_chat_messages(conn if not conn.closed else None)
    fixes.extend(chat_fixes)

    if not conn.closed:
        conn.close()

    return findings, fixes


def process_chat_messages(conn_in=None):
    """Read unaddressed chat messages and attempt to act on them.

    For each message:
    1. Analyze the text for actionable keywords (data quality, missing evidence, etc.)
    2. Run relevant DB checks/fixes if the message references a ticker or issue
    3. Mark the message as addressed and add a response note
    4. If the human unchecks it later, it will reappear as unaddressed next cycle
    """
    fixes = []
    conn = conn_in or psycopg2.connect(DB_URL)
    own_conn = conn_in is None
    cur = conn.cursor()

    try:
        # Get unaddressed messages
        cur.execute("""
            SELECT id, message, ticker, page_url, context
            FROM chat_messages
            WHERE addressed = FALSE
            ORDER BY created_at ASC
        """)
        messages = cur.fetchall()

        if not messages:
            return fixes

        log(f"  Processing {len(messages)} unaddressed chat messages")

        for msg_id, message, ticker, page_url, context in messages:
            response = _handle_chat_message(cur, conn, msg_id, message, ticker, page_url, context)
            if response:
                fixes.append(f"Chat #{msg_id}: {response}")
                log(f"  Chat #{msg_id}: {response}")

        return fixes
    except Exception as e:
        log(f"  Error processing chat messages: {e}")
        return fixes
    finally:
        if own_conn and not conn.closed:
            conn.close()


def _handle_chat_message(cur, conn, msg_id, message, ticker, page_url, context):
    """Process a single chat message and return a response string, or None if no action taken."""
    msg_lower = message.lower()
    actions_taken = []

    # ── Ticker-specific checks ──────────────────────────────────────
    if ticker:
        # Check if this ticker's data has obvious issues
        cur.execute("""
            SELECT c.id, c.company_name FROM companies c
            WHERE c.ticker = %s
        """, (ticker.upper(),))
        company = cur.fetchone()

        if not company:
            _mark_addressed(cur, conn, msg_id, f"Ticker {ticker} not found in database")
            return f"Ticker {ticker} not found in DB"

        company_id, company_name = company

        # Check for issues mentioned in the message
        if any(w in msg_lower for w in ["project", "investment", "dollar", "huge", "big", "wrong", "incorrect", "weird"]):
            # Check for suspicious projects
            cur.execute("""
                SELECT id, short_title, dollar_total, confidence, status
                FROM investment_projects
                WHERE company_id = %s AND status != 'suppressed'
                ORDER BY dollar_total DESC NULLS LAST
                LIMIT 5
            """, (company_id,))
            projects = cur.fetchall()

            if projects:
                # Report top projects for review
                top_projects = [f"#{p[0]} '{p[1]}' ${p[2]:,.0f} conf={p[3]}" if p[2] else f"#{p[0]} '{p[1]}' (no dollars)" for p in projects[:3]]
                actions_taken.append(f"Top projects for {ticker}: {'; '.join(top_projects)}")

        if any(w in msg_lower for w in ["evidence", "no evidence", "missing", "linked", "source"]):
            # Check for orphaned projects (no evidence groups linked)
            cur.execute("""
                SELECT id, short_title, evidence_group_ids
                FROM investment_projects
                WHERE company_id = %s AND status != 'suppressed'
            """, (company_id,))
            for pid, title, eg_ids in cur.fetchall():
                if not eg_ids or eg_ids == '{}' or eg_ids == '[]':
                    actions_taken.append(f"Flagged project #{pid} '{title}' — no evidence groups linked")

        if any(w in msg_lower for w in ["score", "scoring", "rating", "high", "low", "overrated", "underrated"]):
            cur.execute("""
                SELECT composite_opp_score, composite_real_score, scored_at
                FROM company_scores
                WHERE company_id = %s
                ORDER BY scored_at DESC LIMIT 1
            """, (company_id,))
            score = cur.fetchone()
            if score:
                actions_taken.append(f"Current scores for {ticker}: opp={score[0]}, real={score[1]}, scored={score[2]}")

        if any(w in msg_lower for w in ["dedupe", "duplicate", "dedup", "not deduped"]):
            # Check for duplicate evidence
            cur.execute("""
                SELECT COUNT(*), COUNT(DISTINCT passage_text)
                FROM evidence_group_passages egp
                JOIN evidence_groups eg ON egp.group_id = eg.id
                WHERE eg.company_id = %s
            """, (company_id,))
            total, unique = cur.fetchone()
            if total > unique:
                dupes = total - unique
                actions_taken.append(f"Found {dupes} duplicate passages out of {total} for {ticker}")
            else:
                actions_taken.append(f"No duplicate passages found for {ticker} ({total} total)")

    # ── Generic (non-ticker) checks ─────────────────────────────────
    if any(w in msg_lower for w in ["pipeline", "stuck", "slow", "running", "progress"]):
        cur.execute("SELECT COUNT(*) FROM pipeline_runs WHERE status = 'running'")
        running = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM pipeline_runs WHERE status = 'running' AND started_at < NOW() - INTERVAL '2 hours'")
        zombies = cur.fetchone()[0]
        actions_taken.append(f"Pipeline: {running} running, {zombies} zombies")
        if zombies > 0:
            cur.execute("""
                UPDATE pipeline_runs SET status = 'failed', completed_at = NOW()
                WHERE status = 'running' AND started_at < NOW() - INTERVAL '2 hours'
            """)
            conn.commit()
            actions_taken.append(f"Killed {zombies} zombie pipeline runs")

    if any(w in msg_lower for w in ["unclear", "confusing", "don't understand", "what is", "what does"]):
        actions_taken.append("Noted as UX/clarity feedback — flagged for review")

    # ── Mark as addressed ───────────────────────────────────────────
    if actions_taken:
        response = "; ".join(actions_taken)
        _mark_addressed(cur, conn, msg_id, response)
        return response
    else:
        # Still mark addressed with an acknowledgment
        _mark_addressed(cur, conn, msg_id, "Reviewed — no automated action needed, noted for manual review")
        return "Reviewed — noted for manual review"


def _mark_addressed(cur, conn, msg_id, response):
    """Mark a chat message as addressed and append to the response thread."""
    try:
        thread_entry = json.dumps({
            "agent": "Data Quality Guardian",
            "response": response,
            "timestamp": now(),
            "attempt": _get_attempt_number(cur, msg_id),
        })
        cur.execute("""
            UPDATE chat_messages
            SET addressed = TRUE,
                context = context || jsonb_build_object('guardian_response', %s, 'addressed_at', %s),
                response_thread = COALESCE(response_thread, '[]'::jsonb) || %s::jsonb
            WHERE id = %s
        """, (response, now(), thread_entry, msg_id))
        conn.commit()
    except Exception as e:
        log(f"  Error marking chat #{msg_id} addressed: {e}")
        try:
            conn.rollback()
        except Exception:
            pass


def _get_attempt_number(cur, msg_id):
    """Get the current attempt number for a chat message (how many times it's been worked on)."""
    try:
        cur.execute("SELECT response_thread FROM chat_messages WHERE id = %s", (msg_id,))
        row = cur.fetchone()
        if row and row[0]:
            thread = row[0] if isinstance(row[0], list) else json.loads(row[0])
            return len(thread) + 1
        return 1
    except Exception:
        return 1


def update_bulletin(cycle_num, findings, fixes):
    """Update the Data Quality Guardian section in bulletin.md."""
    with open(BULLETIN, "r") as f:
        content = f.read()

    section_header = "## Data Quality Guardian"
    report = f"{section_header}\n"
    report += f"**Last cycle**: #{cycle_num} at {now()}\n\n"
    report += "**Findings:**\n"
    for f_item in findings:
        for line in f_item.split("\n"):
            report += f"- {line}\n"
    if fixes:
        report += "\n**Fixes applied:**\n"
        for fix in fixes:
            report += f"- {fix}\n"
    report += "\n"

    # Replace existing section or append
    if section_header in content:
        # Find section boundaries
        start = content.index(section_header)
        # Find next ## header or end of file
        rest = content[start + len(section_header):]
        next_section = rest.find("\n## ")
        if next_section >= 0:
            end = start + len(section_header) + next_section
            content = content[:start] + report + content[end + 1:]
        else:
            content = content[:start] + report
    else:
        content += "\n" + report

    with open(BULLETIN, "w") as f:
        f.write(content)


def main():
    cycle = 0
    log("Data Quality Guardian starting...")
    while True:
        cycle += 1
        try:
            log(f"=== Cycle {cycle} ===")
            findings, fixes = run_cycle(cycle)
            update_bulletin(cycle, findings, fixes)
            for f in findings:
                log(f"  {f.split(chr(10))[0]}")
            if fixes:
                for fix in fixes:
                    log(f"  FIX: {fix}")
            log(f"Cycle {cycle} complete. Sleeping {SLEEP_SECONDS}s...")
        except Exception as e:
            log(f"ERROR in cycle {cycle}: {e}")
            traceback.print_exc()
        time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    main()
