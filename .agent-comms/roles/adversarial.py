"""Adversarial agent — challenges claims with specific, data-aware critiques."""

import logging
import re

from .base import BaseAgent

logger = logging.getLogger(__name__)


def _make_challenge(finding_text: str) -> str:
    """Generate a specific challenge based on the finding content."""
    text_lower = finding_text.lower()

    # Extract numbers for reference
    numbers = re.findall(r'[\d,]+\.?\d*', finding_text)

    if "0" in finding_text and "average realization" in text_lower:
        return ("The average realization score being 0 is a major red flag. "
                "That means none of our companies are showing real-world AI adoption. "
                "Is this a data problem or a scoring methodology problem? "
                "We should check if the realization pipeline is even running.")

    if "critical" in text_lower or "stuck" in text_lower:
        return ("Before we panic about this, let's separate correlation from causation. "
                "Are these companies genuinely stuck, or do they just have less public data? "
                "Small-cap companies might have fewer filings and news mentions. "
                "Can we control for company size?")

    if "calibration" in text_lower and "stddev" in text_lower:
        return ("The stddev tells us about spread, but not about accuracy. "
                "What we really need is a calibration curve — are companies with 80% capture_probability "
                "actually being captured ~80% of the time? Without ground truth data, "
                "these statistics are meaningless.")

    if "quadrant" in text_lower and "high_opp_low_real" in text_lower:
        return ("If most companies are in high_opp_low_real, that's either a genuine market signal "
                "(lots of AI opportunity but early days for real adoption) or it means our realization "
                "scoring is broken. Can you check if realization scores are correlated with "
                "actual product launches from the product_analysis pipeline?")

    if "coverage" in text_lower and "valuation" in text_lower:
        if numbers:
            return (f"With {numbers[0] if numbers else 'that many'} companies having valuations, "
                    "what's the overlap with our scored companies? If we're scoring companies "
                    "that don't have valuations, or valuing companies we haven't scored, "
                    "that's a data alignment issue.")

    if "provenance" in text_lower or "url" in text_lower:
        return ("Missing URLs aren't just a completeness issue — they prevent users from "
                "verifying our claims. Have we checked if the missing URLs correlate with "
                "specific source types? News articles should always have URLs.")

    if "discrepan" in text_lower:
        return ("Discrepancies aren't necessarily bad — they could mean we have multiple "
                "perspectives on the same evidence. But are these discrepancies between "
                "high-authority sources or low-authority ones? That distinction matters.")

    if "engagement" in text_lower or "chat" in text_lower:
        return ("Response rate is a vanity metric. What matters is whether our responses "
                "actually helped users. Are users who chat coming back? "
                "Are they rating companies after chatting?")

    if "rating" in text_lower:
        return ("User ratings tell us about perceived quality, not actual accuracy. "
                "Have we checked if highly-rated companies actually performed better? "
                "Without that validation, ratings are just sentiment.")

    if "stale" in text_lower:
        return ("How are we defining 'stale'? 30 days might be fine for a large-cap with "
                "stable fundamentals, but too long for a fast-moving AI startup. "
                "We should think about adaptive freshness thresholds.")

    if "top" in text_lower and ("opportunity" in text_lower or "movers" in text_lower):
        return ("The top movers list is only useful if it changes over time. "
                "If the same companies keep showing up, either they're genuinely the best "
                "opportunities or our scoring has a bias. How stable is this list week-over-week?")

    if "evidence" in text_lower and "freshness" in text_lower:
        return ("Freshness metrics by dimension are helpful, but what about the interaction "
                "between dimensions? If cost evidence is fresh but revenue evidence is stale, "
                "our net-opportunity calculations could be way off.")

    # Generic but still specific fallback
    return ("These numbers paint a picture, but I'm not convinced we're looking at the right thing. "
            "What's the trend over time? A snapshot doesn't tell us if things are getting better or worse.")


class Adversarial(BaseAgent):
    def __init__(self, team_name: str, agent_name: str):
        super().__init__(agent_name=agent_name, role="adversarial", team_name=team_name)
        self._challenged_ids: set[int] = set()
        self._acknowledged_ids: set[int] = set()
        self._recent_challenges: list[str] = []  # track last N challenge texts to avoid repetition

    async def run_cycle(self, cycle_num: int):
        channel = f"#{self.team_name}"

        messages = self.read_others_messages(channel)

        for msg in messages:
            msg_id = msg["id"]
            sender_role = msg.get("sender_role", "")
            content = msg["content"]

            # Challenge NEW findings with specific critique (skip if we'd repeat ourselves)
            if msg.get("message_type") == "finding" and msg_id not in self._challenged_ids:
                self._challenged_ids.add(msg_id)
                challenge = _make_challenge(content)
                # Don't post the same challenge text we posted recently
                if challenge in self._recent_challenges:
                    logger.info("[%s] Skipping duplicate challenge for #%d", self.agent_name, msg_id)
                    continue
                self._recent_challenges.append(challenge)
                if len(self._recent_challenges) > 5:
                    self._recent_challenges.pop(0)
                self.post_message(
                    channel,
                    f"@{msg['sender_name']} {challenge}",
                    msg_type="chat",
                )
                logger.info("[%s] Challenged finding #%d", self.agent_name, msg_id)
                break  # one per cycle

            # Acknowledge investigator's clarification (once per message)
            if sender_role == "investigator" and self.agent_name in content and msg_id not in self._acknowledged_ids:
                self._acknowledged_ids.add(msg_id)
                # Make the acknowledgment reference what they said
                if "calibration" in content.lower():
                    ack = "Good data on calibration. The spread looks reasonable but I'd still want to validate against outcomes."
                elif "quadrant" in content.lower():
                    ack = "The quadrant data confirms the skew. That strengthens the case for investigating realization scoring."
                elif "coverage" in content.lower():
                    ack = "Coverage numbers are helpful. Let's make sure we're not double-counting across evidence groups."
                elif "stale" in content.lower() or "freshness" in content.lower():
                    ack = "The freshness data gives us a baseline. We should track this metric weekly."
                else:
                    ack = "That additional data helps. I'm satisfied this finding is grounded, but let's keep monitoring."
                self.post_message(
                    channel,
                    f"@{msg['sender_name']} {ack}",
                    msg_type="chat",
                )
                break
