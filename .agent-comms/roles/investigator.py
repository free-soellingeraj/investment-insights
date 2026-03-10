"""Investigator agent — queries DB for data insights ONLY when directed by teammates."""

import logging

from sqlalchemy import text

from ai_opportunity_index.storage.db import get_session
from .base import BaseAgent

logger = logging.getLogger(__name__)

# Investigation queries keyed by topic keywords
QUERY_CATALOG = {
    # Core optimization queries
    "scoring_stats": {
        "sql": """
            SELECT COUNT(*) as cnt,
                   ROUND(AVG(composite_opp_score)::numeric, 3) as avg_opp,
                   ROUND(AVG(composite_real_score)::numeric, 3) as avg_real,
                   ROUND(AVG(opportunity)::numeric, 3) as avg_opportunity
            FROM company_scores WHERE scored_at > NOW() - INTERVAL '24 hours'
        """,
        "fmt": lambda r: (
            f"Scoring stats (24h): {r[0]} scores computed. "
            f"Average opportunity score: {r[1] or 0} (scale 0-1). "
            f"Average realization score: {r[2] or 0}. "
            f"Average combined opportunity: {r[3] or 0}. "
            f"{'High volume day.' if r[0] and r[0] > 2000 else 'Low volume day.'}"
        ),
        "keywords": ["scoring", "scores", "opportunity", "stats"],
    },
    "quadrant_distribution": {
        "sql": "SELECT quadrant, COUNT(*) FROM company_scores WHERE scored_at > NOW() - INTERVAL '7 days' GROUP BY quadrant ORDER BY COUNT(*) DESC",
        "fmt": lambda rows: (
            "Quadrant distribution (7d): " + ", ".join(f"{r[0] or 'NULL'}={r[1]}" for r in rows)
            + f". Total: {sum(r[1] for r in rows)} scores. "
            + ("CONCERN: Most companies in high_opp_low_real — realization scores may need recalibration." if rows and rows[0][0] == "high_opp_low_real" and rows[0][1] > sum(r[1] for r in rows[1:]) else "Distribution looks balanced.")
        ),
        "multi": True,
        "keywords": ["quadrant", "distribution", "realization", "disconnect"],
    },
    "top_movers": {
        "sql": """
            SELECT c.ticker, cs.composite_opp_score, cs.opportunity, cs.capture_probability
            FROM company_scores cs JOIN companies c ON c.id = cs.company_id
            WHERE cs.scored_at > NOW() - INTERVAL '24 hours'
            ORDER BY cs.opportunity DESC LIMIT 10
        """,
        "fmt": lambda rows: (
            "Top 10 by opportunity (24h): " + ", ".join(
                f"{r[0]}(opp={r[2]:.2f}, capture={r[3]:.2f})" if r[3] is not None
                else f"{r[0]}(opp={r[2]:.2f}, capture=N/A)" for r in rows
            )
            + f". {sum(1 for r in rows if r[3] and r[3] > 0.5)}/{len(rows)} have capture_probability > 0.5."
        ),
        "multi": True,
        "keywords": ["top", "movers", "highest", "best", "opportunity"],
    },
    "calibration": {
        "sql": """
            SELECT ROUND(AVG(capture_probability)::numeric, 3),
                   ROUND(STDDEV(capture_probability)::numeric, 3),
                   COUNT(*),
                   ROUND(MIN(capture_probability)::numeric, 3),
                   ROUND(MAX(capture_probability)::numeric, 3)
            FROM company_scores WHERE capture_probability IS NOT NULL
            AND scored_at > NOW() - INTERVAL '7 days'
        """,
        "fmt": lambda r: (
            f"Calibration stats (7d): avg capture_probability={r[0] or 0}, stddev={r[1] or 0}, "
            f"range=[{r[3] or 0}, {r[4] or 0}], n={r[2]}. "
            + ("ISSUE: Low stddev suggests probabilities are clustering — may need Platt scaling adjustment." if r[1] and float(r[1]) < 0.1 else "")
            + ("ISSUE: Values near 0 or 1 suggest poor calibration." if r[0] and (float(r[0]) < 0.1 or float(r[0]) > 0.9) else "")
            + ("Calibration looks reasonable." if r[1] and float(r[1]) >= 0.1 and r[0] and 0.1 <= float(r[0]) <= 0.9 else "")
        ),
        "keywords": ["calibration", "capture", "probability", "clustering"],
    },
    "valuation_coverage": {
        "sql": """
            SELECT COUNT(DISTINCT eg.company_id) as companies_with_valuations,
                   COUNT(*) as total_valuations,
                   ROUND(AVG(v.confidence)::numeric, 3) as avg_confidence,
                   COUNT(CASE WHEN v.confidence < 0.3 THEN 1 END) as low_conf
            FROM valuations v JOIN evidence_groups eg ON eg.id = v.group_id
            WHERE v.created_at > NOW() - INTERVAL '7 days'
        """,
        "fmt": lambda r: (
            f"Valuation coverage (7d): {r[0]} companies have valuations, {r[1]} total valuations, "
            f"avg confidence={r[2] or 0}. Low-confidence valuations (<0.3): {r[3]}. "
            + (f"WARNING: {r[3]} low-confidence valuations may be adding noise." if r[3] and r[3] > 100 else "")
        ),
        "keywords": ["valuation", "coverage", "confidence", "dollars", "evidence_dollars"],
    },
    "missing_urls": {
        "sql": """
            SELECT COUNT(*) as missing_url,
                   (SELECT COUNT(*) FROM evidence_group_passages) as total,
                   (SELECT COUNT(*) FROM evidence_group_passages WHERE source_publisher IS NULL AND source_url IS NOT NULL) as missing_pub
        """,
        "fmt": lambda r: (
            f"Passage provenance: {r[0]} passages missing URLs out of {r[1]} total ({100*r[0]//max(r[1],1)}% gap). "
            f"{r[2]} have URLs but no publisher attribution. "
            + ("Provenance coverage is good." if r[0] < r[1] * 0.05 else "CONCERN: Significant provenance gaps remain.")
        ),
        "keywords": ["url", "provenance", "missing", "passages", "publisher"],
    },
    "evidence_freshness": {
        "sql": """
            SELECT target_dimension, COUNT(*),
                   ROUND(AVG(mean_confidence)::numeric, 3),
                   COUNT(CASE WHEN created_at < NOW() - INTERVAL '30 days' THEN 1 END) as stale
            FROM evidence_groups
            WHERE created_at > NOW() - INTERVAL '90 days'
            GROUP BY target_dimension ORDER BY COUNT(*) DESC
        """,
        "fmt": lambda rows: (
            "Evidence freshness (90d) by dimension: " + "; ".join(
                f"{r[0] or 'general'}: {r[1]} groups (conf={r[2] or 0}, {r[3]} stale >30d)" for r in rows
            ) + ". " + (
                "CONCERN: Many stale evidence groups." if any(r[3] > r[1] * 0.5 for r in rows) else "Freshness looks acceptable."
            )
        ),
        "multi": True,
        "keywords": ["evidence", "freshness", "stale", "dimension", "groups"],
    },
    "discrepancies": {
        "sql": "SELECT COUNT(*), COUNT(DISTINCT company_id) FROM valuation_discrepancies",
        "fmt": lambda r: (
            f"Valuation discrepancies: {r[0]} discrepancies across {r[1]} companies. "
            + (f"HIGH: {r[0]} unresolved discrepancies is concerning — suggests conflicting evidence sources." if r[0] > 1000 else "Discrepancy count is manageable.")
        ),
        "keywords": ["discrepancy", "discrepancies", "conflict", "conflicting"],
    },
    "stale_scores": {
        "sql": """
            SELECT COUNT(*) FROM companies c
            WHERE c.is_active AND NOT EXISTS (
                SELECT 1 FROM company_scores cs WHERE cs.company_id = c.id
                AND cs.scored_at > NOW() - INTERVAL '30 days'
            )
        """,
        "fmt": lambda r: (
            f"Companies with stale scores (>30d): {r[0]}. "
            + ("CRITICAL: Large number of active companies have outdated scores. Pipeline may be stuck." if r[0] > 5000 else "")
            + ("Score freshness is acceptable." if r[0] < 1000 else "")
        ),
        "keywords": ["stale", "outdated", "pipeline", "stuck", "30 day"],
    },
    "chat_activity": {
        "sql": """
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN addressed THEN 1 ELSE 0 END) as addressed
            FROM chat_messages WHERE created_at > NOW() - INTERVAL '24 hours'
        """,
        "fmt": lambda r: (
            f"User engagement (24h): {r[0]} chat messages, {r[1] or 0} addressed. "
            + (f"Response rate: {100*(r[1] or 0)//max(r[0],1)}%." if r[0] else "No chat activity.")
        ),
        "keywords": ["chat", "user", "engagement", "messages", "activity"],
    },
    "rating_activity": {
        "sql": """
            SELECT COUNT(*), ROUND(AVG(rating)::numeric, 2),
                   COUNT(CASE WHEN rating > 0 THEN 1 END) as positive,
                   COUNT(CASE WHEN rating < 0 THEN 1 END) as negative
            FROM human_ratings WHERE created_at > NOW() - INTERVAL '7 days'
        """,
        "fmt": lambda r: (
            f"User ratings (7d): {r[0]} total, avg={r[1] or 'N/A'}, "
            f"{r[2]} positive, {r[3]} negative. "
            + ("Users are mostly rating positively." if r[2] and r[3] and r[2] > r[3] * 2 else "")
            + ("Mixed sentiment from users." if r[2] and r[3] and r[2] <= r[3] * 2 else "")
        ),
        "keywords": ["rating", "ratings", "sentiment", "user feedback", "positive", "negative"],
    },
    "active_companies": {
        "sql": """
            SELECT COUNT(*) as active,
                   (SELECT COUNT(*) FROM companies) as total,
                   (SELECT COUNT(DISTINCT company_id) FROM company_scores WHERE scored_at > NOW() - INTERVAL '24 hours') as scored_24h
        """,
        "fmt": lambda r: (
            f"Company coverage: {r[0]} active companies out of {r[1]} total. "
            f"{r[2]} scored in last 24h ({100*r[2]//max(r[0],1)}% daily coverage). "
            + ("Good daily coverage." if r[2] > r[0] * 0.3 else "LOW daily coverage — pipeline may need attention.")
        ),
        "keywords": ["companies", "active", "coverage", "total", "how many"],
    },
}

# Default investigation sequence per team
TEAM_DEFAULT_TOPICS = {
    "core-optimization": ["scoring_stats", "calibration", "quadrant_distribution", "top_movers", "valuation_coverage"],
    "data-integrity": ["missing_urls", "evidence_freshness", "discrepancies", "stale_scores", "valuation_coverage"],
    "ui-team": ["chat_activity", "rating_activity", "active_companies", "scoring_stats", "stale_scores"],
}


class Investigator(BaseAgent):
    def __init__(self, team_name: str, agent_name: str):
        super().__init__(agent_name=agent_name, role="investigator", team_name=team_name)
        self._last_finding: str | None = None
        self._query_index = 0
        self._responded_to: set[int] = set()  # message IDs we've already responded to

    async def run_cycle(self, cycle_num: int):
        channel = f"#{self.team_name}"

        # Read team messages — only react if someone asked us to do something
        team_msgs = self.read_others_messages(channel)

        directed = False
        for msg in team_msgs:
            msg_id = msg["id"]
            if msg_id in self._responded_to:
                continue

            content_lower = msg["content"].lower()
            sender_role = msg.get("sender_role", "")

            # React to idea_guy directives
            if sender_role == "idea_guy" and (
                "investigator" in content_lower or "look into" in content_lower
                or "can you check" in content_lower or "please check" in content_lower
                or "dig deeper" in content_lower
            ):
                self._responded_to.add(msg_id)
                # Find best matching query for what they asked
                finding = self._targeted_query(msg["content"])
                if finding:
                    self.post_message(
                        channel,
                        finding,
                        msg_type="finding",
                    )
                    self._last_finding = finding
                    directed = True
                    logger.info("[%s] Finding for %s: %s", self.agent_name, msg["sender_name"], finding[:60])
                break  # one response per cycle

            # React to adversarial challenges with a brief clarification (NOT a new finding)
            if sender_role == "adversarial" and "?" in content_lower and msg_id not in self._responded_to:
                self._responded_to.add(msg_id)
                # Post a brief data-driven clarification
                clarification = self._clarify_challenge(msg["content"])
                if clarification:
                    self.post_message(
                        channel,
                        f"@{msg['sender_name']} {clarification}",
                        msg_type="chat",
                    )
                    directed = True
                break  # one response per cycle

            # React to human messages
            if msg.get("message_type") == "human_input":
                self._responded_to.add(msg_id)
                finding = self._targeted_query(msg["content"])
                if finding:
                    self.post_message(
                        channel,
                        f"@{msg['sender_name']} Here's what I found: {finding}",
                        msg_type="finding",
                    )
                    directed = True
                break

        # If nobody directed us, run default team investigation (first cycle only to seed data)
        if not directed and cycle_num == 1:
            finding = self._default_investigation()
            if finding and finding != self._last_finding:
                self.post_message(channel, finding, msg_type="finding")
                self._last_finding = finding
                logger.info("[%s] Initial finding: %s", self.agent_name, finding[:60])

    def _targeted_query(self, request_text: str) -> str | None:
        """Find and run the best matching query for a request."""
        request_lower = request_text.lower()

        best_match = None
        best_score = 0
        for key, qdef in QUERY_CATALOG.items():
            score = sum(1 for kw in qdef["keywords"] if kw in request_lower)
            if score > best_score:
                best_score = score
                best_match = key

        # If no keyword match, use next default query
        if not best_match or best_score == 0:
            return self._default_investigation()

        return self._run_query(best_match)

    def _default_investigation(self) -> str | None:
        """Run the next query in the team's default sequence."""
        topics = TEAM_DEFAULT_TOPICS.get(self.team_name, list(QUERY_CATALOG.keys())[:5])
        topic = topics[self._query_index % len(topics)]
        self._query_index += 1
        return self._run_query(topic)

    def _run_query(self, query_key: str) -> str | None:
        """Execute a specific query by key."""
        qdef = QUERY_CATALOG.get(query_key)
        if not qdef:
            return None

        session = get_session()
        try:
            if qdef.get("multi"):
                rows = session.execute(text(qdef["sql"])).fetchall()
                return qdef["fmt"](rows) if rows else None
            else:
                row = session.execute(text(qdef["sql"])).fetchone()
                return qdef["fmt"](row) if row else None
        except Exception as e:
            logger.error("[%s] Query error (%s): %s", self.agent_name, query_key, e)
            return None
        finally:
            session.close()

    def _clarify_challenge(self, challenge_text: str) -> str | None:
        """Provide a brief data-driven response to an adversarial challenge."""
        challenge_lower = challenge_text.lower()

        if "confidence" in challenge_lower or "interval" in challenge_lower:
            return self._run_query("calibration")
        elif "stale" in challenge_lower or "artifact" in challenge_lower:
            return self._run_query("evidence_freshness")
        elif "significant" in challenge_lower or "noise" in challenge_lower:
            return self._run_query("calibration")
        elif "compare" in challenge_lower or "last week" in challenge_lower:
            return self._run_query("scoring_stats")
        elif "sector" in challenge_lower or "bias" in challenge_lower:
            return self._run_query("quadrant_distribution")
        elif "sources" in challenge_lower or "verified" in challenge_lower:
            return self._run_query("valuation_coverage")
        elif "outlier" in challenge_lower or "exclude" in challenge_lower:
            return self._run_query("top_movers")

        return None  # Don't respond if we can't provide relevant data
