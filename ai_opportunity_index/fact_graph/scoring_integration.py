"""Integration between cross-source verification and the scoring pipeline.

Applies confidence adjustments from verification to company scores.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ai_opportunity_index.fact_graph.verification import VerificationResult

logger = logging.getLogger(__name__)

# Factor score keys in the aggregated scores dict (from aggregate_valuations)
_FACTOR_SCORE_KEYS = ("cost_score", "revenue_score", "general_score")

# Limits on how much verification can move factor scores
_MAX_BOOST_PCT = 0.10   # high agreement → up to +10%
_MAX_PENALTY_PCT = 0.20  # high disagreement → up to -20%


def apply_verification_adjustment(
    base_scores: dict,
    verification_result: VerificationResult,
) -> dict:
    """Apply cross-source verification results to adjust scores.

    Takes the raw aggregated scores dict (from aggregate_valuations) and
    adjusts factor scores based on source agreement/disagreement.

    - High agreement -> small boost to confidence (up to +10% on factor scores)
    - High disagreement -> penalty (up to -20% on factor scores)
    - Adds verification metadata to the result dict

    Args:
        base_scores: Dict produced by ``aggregate_valuations`` containing
            ``cost_score``, ``revenue_score``, ``general_score``, and dollar
            fields.
        verification_result: A ``VerificationResult`` from
            ``CrossSourceVerifier`` or ``verify_company_evidence``.

    Returns:
        A *new* dict with the same keys as *base_scores* plus adjusted factor
        scores and verification metadata fields.
    """
    adjusted = dict(base_scores)

    confidence_adj = verification_result.confidence_adjustment

    # Determine the effective multiplier, clamped to our limits.
    # confidence_adjustment > 1.0 means confirmations dominate (boost),
    # < 1.0 means contradictions dominate (penalty).
    if confidence_adj >= 1.0:
        # Boost: cap at +MAX_BOOST_PCT (e.g. 1.0 -> 1.10)
        effective_multiplier = min(confidence_adj, 1.0 + _MAX_BOOST_PCT)
    else:
        # Penalty: floor at -MAX_PENALTY_PCT (e.g. 1.0 -> 0.80)
        effective_multiplier = max(confidence_adj, 1.0 - _MAX_PENALTY_PCT)

    for key in _FACTOR_SCORE_KEYS:
        if key not in adjusted:
            continue
        raw = adjusted[key]
        new_val = raw * effective_multiplier
        # Factor scores are 0-1; keep them in range
        adjusted[key] = round(min(1.0, max(0.0, new_val)), 4)

    # Attach verification metadata
    adjusted["agreement_score"] = round(verification_result.agreement_score, 4)
    adjusted["num_confirmations"] = len(verification_result.confirmations)
    adjusted["num_contradictions"] = len(verification_result.contradictions)
    adjusted["verification_applied"] = True

    logger.info(
        "Verification adjustment applied: multiplier=%.4f, "
        "agreement=%.2f, confirmations=%d, contradictions=%d",
        effective_multiplier,
        verification_result.agreement_score,
        len(verification_result.confirmations),
        len(verification_result.contradictions),
    )

    return adjusted
