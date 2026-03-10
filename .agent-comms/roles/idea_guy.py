"""Idea Guy agent — directs investigations, synthesizes findings, creates detailed plans."""

import logging

from sqlalchemy import text

from ai_opportunity_index.storage.db import get_session
from .base import BaseAgent
from .pr_helper import create_plan_pr, check_pr_status

logger = logging.getLogger(__name__)

# Directed investigations — specific, actionable requests
INVESTIGATION_SEQUENCES = {
    "core-optimization": [
        "Investigator, can you check the scoring stats for the last 24 hours? I want to see if our opportunity scores are trending differently than expected.",
        "Investigator, please look into the calibration of capture_probability values — are they clustering near 0 or 1? That would indicate poor calibration.",
        "Investigator, can you check the quadrant distribution? If most companies are in high_opp_low_real, our realization scoring may have a systematic issue.",
        "Investigator, please pull the top movers by opportunity score. I want to see if the same tickers keep appearing or if there's healthy rotation.",
        "Investigator, can you check valuation coverage? I need to know how many companies have dollar-denominated evidence and what the confidence looks like.",
    ],
    "data-integrity": [
        "Investigator, can you check how many passages are missing source URLs? Provenance gaps undermine our credibility.",
        "Investigator, please look into evidence freshness by dimension — how many evidence groups are stale (>30 days)?",
        "Investigator, can you check the valuation discrepancy count? I want to know if conflicting evidence is accumulating.",
        "Investigator, please check how many active companies have stale scores (>30 days). If the number is high, the pipeline may be stuck.",
        "Investigator, can you look at which source types have the highest confidence? We may need to adjust authority weights.",
    ],
    "ui-team": [
        "Investigator, can you check user chat activity for the last 24 hours? I want to see our response rate.",
        "Investigator, please look at the rating distribution — are users mostly rating positively or negatively?",
        "Investigator, can you check how many active companies we have and what percentage got scored today?",
        "Investigator, please check which companies have stale scores. Users seeing outdated data is a bad experience.",
        "Investigator, can you look at the scoring stats? I want to make sure the numbers we're showing users are fresh.",
    ],
}


class IdeaGuy(BaseAgent):
    def __init__(self, team_name: str, agent_name: str):
        super().__init__(agent_name=agent_name, role="idea_guy", team_name=team_name)
        self._findings: list[dict] = []  # {content, sender, challenged, challenge_text}
        self._directive_index = 0
        self._synthesis_count = 0
        self._waiting_for_response = False
        self._plan_count = 0

    async def run_cycle(self, cycle_num: int):
        channel = f"#{self.team_name}"

        # Check for plan PR merges every 5th cycle
        if cycle_num % 5 == 0:
            self._check_plan_pr_merges(channel)

        messages = self.read_others_messages(channel)

        # Also check cross-team (only escalations from other idea guys)
        cross_msgs = self.read_others_messages("#cross-team")

        has_new_finding = False

        for msg in messages:
            content = msg["content"]
            sender_role = msg.get("sender_role", "")
            msg_type = msg.get("message_type", "")

            # Investigator posted a finding — collect it (skip duplicates)
            if msg_type == "finding" and sender_role == "investigator":
                # Don't collect identical findings we've already seen
                if any(f["content"] == content for f in self._findings):
                    self._waiting_for_response = False
                    continue
                self._findings.append({
                    "content": content,
                    "sender": msg["sender_name"],
                    "challenged": False,
                    "challenge_text": None,
                })
                has_new_finding = True
                self._waiting_for_response = False

            # Adversarial challenged a finding — note the challenge
            elif sender_role == "adversarial":
                for f in reversed(self._findings):
                    if not f["challenged"]:
                        f["challenged"] = True
                        f["challenge_text"] = content
                        break

            # Investigator responded to a challenge — note clarification
            elif sender_role == "investigator" and msg_type == "chat":
                for f in reversed(self._findings):
                    if f["challenged"]:
                        f["challenge_text"] = (f["challenge_text"] or "") + f" → Clarified: {content[:80]}"
                        break

            # Human posted something — this is top priority
            elif msg_type == "human_input":
                self.post_message(
                    channel,
                    f'Team, we have human input: "{content[:200]}". '
                    f"This takes priority over our current investigation. "
                    f"@{self.team_name}-investigator please look into what they're asking about.",
                    msg_type="chat",
                )
                self._waiting_for_response = True

            # Plan status change (from API/human review)
            elif msg_type == "system" and "APPROVED" in content:
                self.post_message(
                    channel,
                    "Great news — our plan has been approved by human review! "
                    f"@{self.team_name}-engineer please claim it and start implementation.",
                    msg_type="chat",
                )

            elif msg_type == "system" and "REJECTED" in content:
                self.post_message(
                    channel,
                    "Plan was rejected by human review. I'll gather more evidence and "
                    "create a stronger proposal. Let me redirect our investigation.",
                    msg_type="chat",
                )
                self._findings.clear()

            # Engineer completed something
            elif sender_role == "engineer" and "code review" in content.lower():
                self.post_message(
                    channel,
                    f"@{msg['sender_name']} nice work on the implementation. "
                    f"@{self.team_name}-code_reviewer can you review this?",
                    msg_type="chat",
                )

            # Code reviewer verified
            elif sender_role == "code_reviewer" and "APPROVED" in content:
                self.post_message(
                    channel,
                    f"Project verified by @{msg['sender_name']}. Good work team. "
                    f"Let me think about what we should investigate next.",
                    msg_type="chat",
                )

        # Decision: what to do this cycle (limit to 1-2 messages per cycle to keep conversation natural)

        # If we have 3+ findings, synthesize and possibly create a plan (this is the most important action)
        if len(self._findings) >= 3:
            self._synthesize_and_plan(channel)
            return

        # Forward important cross-team info (only if no finding to react to, max 1 per cycle)
        if not has_new_finding:
            for msg in cross_msgs[:1]:
                if msg.get("sender_role") == "idea_guy":
                    self.post_message(
                        channel,
                        f"Heads up from {msg['sender_name']}: {msg['content'][:150]}. "
                        f"Let's make sure our findings align with theirs.",
                        msg_type="chat",
                    )
                    return  # Don't do anything else this cycle

        # If we got a new finding, react to it with analysis
        if has_new_finding:
            latest = self._findings[-1]
            content_lower = latest["content"].lower()
            n = len(self._findings)

            # Provide substantive reaction based on what the finding says
            if "critical" in content_lower or "concern" in content_lower or "warning" in content_lower:
                reaction = (
                    f"This is concerning. {latest['content'][:120]}. "
                    f"@{self.team_name}-adversarial what do you think — is this a real problem or noise?"
                )
            elif "good" in content_lower or "acceptable" in content_lower or "reasonable" in content_lower:
                reaction = (
                    f"Good news from the investigation: {latest['content'][:120]}. "
                    f"That's {n} finding(s) collected. Let me think about what else we need."
                )
            elif n < 3:
                reaction = (
                    f"Interesting data point: {latest['content'][:100]}. "
                    f"I have {n} finding(s) so far — need {3 - n} more before I can connect the dots."
                )
            else:
                reaction = f"Got it. That gives us {n} findings to work with. Let me synthesize."

            self.post_message(channel, reaction, msg_type="chat")

            # If we have exactly 2 findings, the next directive should be targeted
            if n == 2 and not self._waiting_for_response:
                self._direct_contextual_investigation(channel)
            return

        # If we're not waiting for a response, direct a new investigation
        if not self._waiting_for_response:
            self._direct_investigation(channel)

    def _direct_investigation(self, channel: str):
        """Ask the investigator to look into something specific."""
        directives = INVESTIGATION_SEQUENCES.get(self.team_name, [])
        if not directives:
            return
        directive = directives[self._directive_index % len(directives)]
        self._directive_index += 1
        self.post_message(channel, directive, msg_type="chat")
        self._waiting_for_response = True

    def _direct_contextual_investigation(self, channel: str):
        """Direct a follow-up investigation based on existing findings."""
        if not self._findings:
            return self._direct_investigation(channel)

        # Analyze what we have to figure out what to ask next
        all_text = " ".join(f["content"] for f in self._findings).lower()

        if "stale" in all_text or "stuck" in all_text:
            msg = (
                f"@{self.team_name}-investigator Based on the stale score data, "
                "I want to understand the pipeline throughput. Can you check the scoring stats "
                "for the last 24 hours? I want to see how many companies we're actually scoring daily."
            )
        elif "calibration" in all_text or "capture_probability" in all_text:
            msg = (
                f"@{self.team_name}-investigator The calibration data is interesting but I need context. "
                "Can you pull the quadrant distribution? If capture_probability looks fine but "
                "most companies are in high_opp_low_real, the issue might be on the realization side."
            )
        elif "quadrant" in all_text and "high_opp_low_real" in all_text:
            msg = (
                f"@{self.team_name}-investigator The quadrant skew towards high_opp_low_real is suspicious. "
                "Can you check the valuation coverage? I want to see if we have enough dollar-denominated "
                "evidence to support these opportunity scores."
            )
        elif "engagement" in all_text or "chat" in all_text:
            msg = (
                f"@{self.team_name}-investigator Good data on user engagement. "
                "Now let's check the rating patterns — are users who chat also rating companies? "
                "That would tell us about user trust."
            )
        elif "provenance" in all_text or "url" in all_text:
            msg = (
                f"@{self.team_name}-investigator We know about provenance gaps. "
                "Can you check evidence freshness by dimension? If stale evidence also has "
                "poor provenance, those are the groups we should deprioritize."
            )
        elif "discrepan" in all_text:
            msg = (
                f"@{self.team_name}-investigator Those discrepancy numbers need context. "
                "Can you check which companies have stale scores? There might be overlap between "
                "high-discrepancy and stale-score companies."
            )
        else:
            return self._direct_investigation(channel)

        self.post_message(channel, msg, msg_type="chat")
        self._waiting_for_response = True

    def _synthesize_and_plan(self, channel: str):
        """Synthesize findings into insights and create a detailed plan."""
        self._synthesis_count += 1

        # Build synthesis from actual findings
        finding_summaries = []
        challenges = []
        for f in self._findings:
            finding_summaries.append(f"- {f['content'][:200]}")
            if f["challenged"] and f["challenge_text"]:
                challenges.append(f"- Challenge: {f['challenge_text'][:150]}")

        synthesis = (
            f"[Synthesis #{self._synthesis_count}] After analyzing {len(self._findings)} findings:\n"
            + "\n".join(finding_summaries[:5])
        )
        if challenges:
            synthesis += "\n\nChallenges raised:\n" + "\n".join(challenges[:3])

        # Determine key insight — only plan if no unreviewed draft exists
        all_text = " ".join(f["content"] for f in self._findings).lower()
        has_pending_plan = self._has_pending_plan()

        if has_pending_plan:
            synthesis += "\n\nConclusion: We already have a plan awaiting review. Continuing to gather data."
            should_plan = False
        elif "concern" in all_text or "critical" in all_text or "issue" in all_text or "warning" in all_text:
            synthesis += "\n\nConclusion: Multiple findings indicate issues that need attention. Creating a plan."
            should_plan = True
        elif self._synthesis_count % 3 == 0:
            synthesis += "\n\nConclusion: Findings suggest an improvement opportunity. Creating a plan."
            should_plan = True
        else:
            synthesis += "\n\nConclusion: Collecting more data before proposing action."
            should_plan = False

        self.post_message(channel, synthesis, msg_type="chat")

        # Escalate to cross-team
        self.post_message(
            "#cross-team",
            f"[{self.team_name}] Synthesis #{self._synthesis_count}: "
            f"Analyzed {len(self._findings)} findings. "
            + ("Action plan incoming." if should_plan else "Monitoring."),
            msg_type="escalation",
        )

        if should_plan:
            self._create_plan(channel)

        self._findings.clear()

    def _create_plan(self, channel: str):
        """Create a detailed plan from actual findings."""
        self._plan_count += 1

        # Build plan text from real findings
        finding_bullets = []
        issues_found = []
        for f in self._findings:
            content = f["content"]
            finding_bullets.append(f"- {content}")
            # Extract issues
            for marker in ["CONCERN:", "CRITICAL:", "ISSUE:", "WARNING:", "HIGH:"]:
                if marker in content:
                    idx = content.index(marker)
                    issues_found.append(content[idx:idx+200].strip())

        # Determine focus area from findings
        all_text = " ".join(f["content"] for f in self._findings).lower()
        if "calibration" in all_text or "capture_probability" in all_text:
            focus = "Scoring Calibration"
            actions = [
                "Review Platt scaling parameters for capture_probability",
                "Check if stddev of capture_probability is too low (clustering issue)",
                "Compare calibration curves across different company sectors",
                "Validate against historical outcomes where available",
                "Update calibration.py scaling factors if needed",
            ]
        elif "stale" in all_text or "outdated" in all_text or "pipeline" in all_text:
            focus = "Pipeline Freshness"
            actions = [
                "Identify companies stuck in scoring pipeline (>30 days without new score)",
                "Check if SEC filing extraction is running for stale companies",
                "Verify news extraction isn't rate-limited or failing silently",
                "Add monitoring for pipeline throughput (scores/hour metric)",
                "Consider priority queue for most-viewed companies",
            ]
        elif "provenance" in all_text or "url" in all_text or "publisher" in all_text:
            focus = "Data Provenance"
            actions = [
                "Backfill missing source URLs for evidence passages",
                "Map source_publisher from URL domains for passages missing attribution",
                "Add provenance validation to evidence extraction pipeline",
                "Create provenance coverage report endpoint for dashboard",
                "Flag low-provenance evidence in UI with warning indicator",
            ]
        elif "discrepan" in all_text or "conflict" in all_text:
            focus = "Evidence Consistency"
            actions = [
                "Audit top companies by discrepancy count",
                "Check if discrepancies correlate with specific source types",
                "Implement automatic discrepancy resolution for low-confidence sources",
                "Add discrepancy trend tracking over time",
                "Review cross-source verification thresholds",
            ]
        elif "chat" in all_text or "user" in all_text or "rating" in all_text or "engagement" in all_text:
            focus = "User Experience"
            actions = [
                "Improve chat response rate by addressing unaddressed messages",
                "Analyze rating patterns to identify scoring accuracy issues",
                "Add loading states for stale data in company profiles",
                "Create user-facing data freshness indicator",
                "Prioritize scoring for companies users are actively viewing",
            ]
        elif "quadrant" in all_text or "distribution" in all_text or "realization" in all_text:
            focus = "Score Distribution"
            actions = [
                "Investigate why realization scores are concentrated at low values",
                "Check if product analysis and GitHub scoring are returning data",
                "Review evidence weighting between opportunity and realization dimensions",
                "Compare quadrant distribution this week vs last week",
                "Adjust composite scoring formula if realization is systematically underweighted",
            ]
        else:
            focus = "General Improvement"
            actions = [
                "Deep investigation into the patterns identified in findings",
                "Cross-reference with other teams' findings for systemic issues",
                "Implement targeted fixes based on root cause analysis",
                "Add monitoring to catch regression",
                "Validate improvements with before/after comparison",
            ]

        challenges_text = ""
        for f in self._findings:
            if f["challenged"] and f["challenge_text"]:
                challenges_text += f"\n- {f['challenge_text'][:200]}"

        title = f"[{self.team_name}] {focus} — Plan #{self._plan_count}"
        plan_text = f"""# {title}

## Context
This plan is based on {len(self._findings)} investigation findings from the {self.team_name} team.

## Key Findings
{chr(10).join(finding_bullets)}

## Issues Identified
{chr(10).join(f'- {issue}' for issue in issues_found) if issues_found else '- No critical issues, but improvement opportunities identified from finding patterns.'}

## Challenges Raised
{challenges_text if challenges_text else '- No adversarial challenges to these findings.'}

## Proposed Actions
{chr(10).join(f'{i+1}. {action}' for i, action in enumerate(actions))}

## Success Criteria
- Measurable improvement in the metrics identified in findings
- No regression in other scoring dimensions
- Changes pass code review and automated tests

## Files Likely Affected
{self._guess_files(focus)}

## Expected Impact
Targeted improvement to {focus.lower()} based on data-driven investigation by the {self.team_name} team.
"""

        session = get_session()
        try:
            team_row = session.execute(
                text("SELECT id FROM agent_teams WHERE name = :name"),
                {"name": self.team_name},
            ).fetchone()
            if not team_row:
                return

            result = session.execute(
                text("""
                    INSERT INTO agent_plans (team_id, title, description, plan_text, status, created_by, created_at, updated_at)
                    VALUES (:team_id, :title, :desc, :plan_text, 'review', :agent_id, NOW(), NOW())
                    RETURNING id
                """),
                {
                    "team_id": team_row[0],
                    "title": title,
                    "desc": f"Data-driven plan based on {len(self._findings)} findings about {focus.lower()}",
                    "plan_text": plan_text,
                    "agent_id": self.agent_id,
                },
            )
            plan_row = result.fetchone()
            session.commit()

            if not plan_row:
                return

            plan_id = plan_row[0]

            # Create a GitHub PR for the plan
            pr_number, pr_url, pr_branch = create_plan_pr(
                self.team_name, plan_id, title, plan_text,
            )

            if pr_number:
                session.execute(
                    text("""
                        UPDATE agent_plans
                        SET pr_number = :pr_number, pr_url = :pr_url, pr_branch = :pr_branch
                        WHERE id = :id
                    """),
                    {"pr_number": pr_number, "pr_url": pr_url, "pr_branch": pr_branch, "id": plan_id},
                )
                session.commit()

            pr_info = f" PR: {pr_url}" if pr_url else ""
            self.post_message(
                channel,
                f'[Plan] Created: "{title}" (plan #{plan_id}).{pr_info} '
                f"Based on {len(self._findings)} findings. "
                f"Proposed {len(actions)} actions targeting {focus.lower()}. "
                f"Merge the PR to approve or close to reject.",
                msg_type="chat",
            )
            logger.info("[%s] Created plan: %s (PR: %s)", self.agent_name, title, pr_url)
        except Exception as e:
            logger.error("[%s] Plan creation error: %s", self.agent_name, e)
            session.rollback()
        finally:
            session.close()

    def _check_plan_pr_merges(self, channel: str):
        """Check if any plan PRs in 'review' status have been merged or closed."""
        session = get_session()
        try:
            rows = session.execute(
                text("""
                    SELECT p.id, p.title, p.pr_number
                    FROM agent_plans p
                    JOIN agent_teams t ON t.id = p.team_id
                    WHERE t.name = :team AND p.status = 'review' AND p.pr_number IS NOT NULL
                """),
                {"team": self.team_name},
            ).fetchall()

            for row in rows:
                plan_id, title, pr_number = row[0], row[1], row[2]
                status = check_pr_status(pr_number)
                if status.get("merged"):
                    session.execute(
                        text("UPDATE agent_plans SET status = 'approved', updated_at = NOW() WHERE id = :id"),
                        {"id": plan_id},
                    )
                    session.commit()
                    self.post_message(
                        channel,
                        f'Plan PR #{pr_number} for "{title}" has been merged! Plan is now approved. '
                        f"@{self.team_name}-engineer please pick it up and start implementation.",
                        msg_type="chat",
                    )
                    logger.info("[%s] Plan %d approved via PR merge", self.agent_name, plan_id)
                elif status.get("state") == "CLOSED":
                    session.execute(
                        text("UPDATE agent_plans SET status = 'rejected', updated_at = NOW() WHERE id = :id"),
                        {"id": plan_id},
                    )
                    session.commit()
                    self.post_message(
                        channel,
                        f'Plan PR #{pr_number} for "{title}" was closed without merging. Plan rejected. '
                        f"I'll gather more evidence and create a stronger proposal.",
                        msg_type="chat",
                    )
                    logger.info("[%s] Plan %d rejected via PR close", self.agent_name, plan_id)
        except Exception as e:
            logger.error("[%s] PR merge check error: %s", self.agent_name, e)
            session.rollback()
        finally:
            session.close()

    def _has_pending_plan(self) -> bool:
        """Check if there's already an active or recently completed plan for this team."""
        session = get_session()
        try:
            # Check for in-progress plans
            active = session.execute(
                text("""
                    SELECT COUNT(*) FROM agent_plans p
                    JOIN agent_teams t ON t.id = p.team_id
                    WHERE t.name = :team AND p.status IN ('draft', 'review', 'approved', 'implementing')
                """),
                {"team": self.team_name},
            ).scalar()
            if active > 0:
                return True

            # Also check for recently completed plans (verified/rejected in last 2 hours)
            # This prevents re-proposing the same topic immediately
            recent = session.execute(
                text("""
                    SELECT COUNT(*) FROM agent_plans p
                    JOIN agent_teams t ON t.id = p.team_id
                    WHERE t.name = :team
                    AND p.status IN ('verified', 'rejected', 'implemented')
                    AND p.updated_at > NOW() - INTERVAL '2 hours'
                """),
                {"team": self.team_name},
            ).scalar()
            return recent > 0
        except Exception:
            return False
        finally:
            session.close()

    def _guess_files(self, focus: str) -> str:
        """Guess which files would be affected based on focus area."""
        file_map = {
            "Scoring Calibration": "- ai_opportunity_index/scoring/calibration.py\n- ai_opportunity_index/scoring/composite.py",
            "Pipeline Freshness": "- ai_opportunity_index/pipeline/runner.py\n- scripts/daily_refresh.py\n- ai_opportunity_index/data/sec_edgar.py",
            "Data Provenance": "- ai_opportunity_index/scoring/evidence_munger.py\n- scripts/backfill_passage_provenance.py",
            "Evidence Consistency": "- ai_opportunity_index/scoring/evidence_valuation.py\n- ai_opportunity_index/fact_graph/verification.py",
            "User Experience": "- web/chat_api.py\n- web/ratings_api.py\n- frontend/src/app/company/[ticker]/page.tsx",
            "Score Distribution": "- ai_opportunity_index/scoring/composite.py\n- ai_opportunity_index/scoring/realization/product_analysis.py",
        }
        return file_map.get(focus, "- To be determined during implementation")
