"""Cross-source verification for investment evidence.

Compares dollar estimates across independent source types for the same
target dimension, identifying confirmations (agreement) and contradictions
(disagreement). Produces a confidence adjustment that can be applied to
base confidence scores.

This is critical for production use with real money: we should trust
claims more when multiple independent sources agree, and flag/reduce
confidence when sources contradict each other.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from itertools import combinations

from pydantic import BaseModel, Field

from ai_opportunity_index.domains import (
    EvidenceGroup,
    TargetDimension,
    Valuation,
    ValuationStage,
)

logger = logging.getLogger(__name__)

# ── Thresholds ────────────────────────────────────────────────────────────

# Two sources agree if min(a,b)/max(a,b) >= 0.5 (within 50%).
AGREEMENT_RATIO_THRESHOLD = 0.5

# Two sources contradict if min(a,b)/max(a,b) < 0.5 (disagree by >100%).
CONTRADICTION_RATIO_THRESHOLD = 0.5

# ── Confidence adjustment constants ──────────────────────────────────────
# Additive adjustments: each confirmation adds a boost, each contradiction
# subtracts a penalty, clamped to [MIN_ADJUSTMENT, MAX_ADJUSTMENT].

CONFIRMATION_BOOST = 0.03    # additive boost per confirmation
MAX_BOOST = 0.1              # ceiling for total confidence adjustment
CONTRADICTION_PENALTY = 0.05  # additive penalty per contradiction
MIN_PENALTY = -0.2           # floor for total confidence adjustment


# ── Models ────────────────────────────────────────────────────────────────


class SourceAgreement(BaseModel):
    """Two independent sources that agree on a dollar estimate."""
    source_a: str
    source_b: str
    dimension: str
    dollar_a: float
    dollar_b: float
    agreement_ratio: float  # min/max, 1.0 = perfect agreement


class SourceDisagreement(BaseModel):
    """Two independent sources that contradict each other."""
    source_a: str
    source_b: str
    dimension: str
    dollar_a: float
    dollar_b: float
    disagreement_ratio: float  # max/min, higher = worse disagreement
    severity: str  # "low", "medium", or "high"


class VerificationResult(BaseModel):
    """Result of cross-source verification for a company.

    Maintains company_id and ticker for backward compatibility with the
    GraphQL resolver layer.
    """
    company_id: int = 0
    ticker: str = ""
    confirmations: list[SourceAgreement] = Field(default_factory=list)
    contradictions: list[SourceDisagreement] = Field(default_factory=list)
    agreement_score: float = 0.0  # 0-1, higher = more agreement
    confidence_adjustment: float = 0.0  # additive: -0.2 to +0.1


# ── Source label helpers ──────────────────────────────────────────────────


def _source_label_from_group(group: EvidenceGroup) -> str:
    """Build a human-readable source label from an evidence group."""
    if group.source_types:
        return "+".join(sorted(group.source_types))
    return "unknown"


def _source_label_from_valuation(valuation: Valuation) -> str:
    """Build a source label from a valuation (uses evidence_type + group_id)."""
    return f"{valuation.evidence_type.value}:group_{valuation.group_id}"


def _dollar_mid_from_valuation(v: Valuation) -> float | None:
    """Extract the midpoint dollar estimate from a valuation."""
    if v.dollar_mid is not None:
        return v.dollar_mid
    if v.dollar_low is not None and v.dollar_high is not None:
        return (v.dollar_low + v.dollar_high) / 2.0
    return None


def _classify_severity(disagreement_ratio: float) -> str:
    """Classify contradiction severity based on disagreement ratio.

    - low:    < 2x disagreement
    - medium: 2-3x disagreement
    - high:   > 3x disagreement
    """
    if disagreement_ratio < 2.0:
        return "low"
    elif disagreement_ratio <= 3.0:
        return "medium"
    else:
        return "high"


# ── Comparison logic ─────────────────────────────────────────────────────


def _compare_pair(
    source_a: str,
    source_b: str,
    dollar_a: float,
    dollar_b: float,
    dimension: str,
) -> SourceAgreement | SourceDisagreement | None:
    """Compare two dollar estimates and classify as agreement or contradiction.

    Returns a SourceAgreement if sources agree (ratio >= threshold),
    a SourceDisagreement if they contradict (ratio < threshold),
    or None if comparison is not meaningful (e.g. zero values).
    """
    if dollar_a <= 0 or dollar_b <= 0:
        return None

    low = min(dollar_a, dollar_b)
    high = max(dollar_a, dollar_b)
    ratio = low / high  # 0 < ratio <= 1.0

    if ratio >= AGREEMENT_RATIO_THRESHOLD:
        return SourceAgreement(
            source_a=source_a,
            source_b=source_b,
            dimension=dimension,
            dollar_a=dollar_a,
            dollar_b=dollar_b,
            agreement_ratio=ratio,
        )
    else:
        disagreement_ratio = high / low
        severity = _classify_severity(disagreement_ratio)
        return SourceDisagreement(
            source_a=source_a,
            source_b=source_b,
            dimension=dimension,
            dollar_a=dollar_a,
            dollar_b=dollar_b,
            disagreement_ratio=disagreement_ratio,
            severity=severity,
        )


# ── Core verifier ────────────────────────────────────────────────────────


class CrossSourceVerifier:
    """Compares dollar estimates across independent sources for the same dimension.

    Takes evidence groups or valuations for a single company and identifies
    cross-source confirmations and contradictions.
    """

    def verify_groups(self, groups: list[EvidenceGroup]) -> VerificationResult:
        """Verify evidence groups by comparing dollar estimates across source types.

        Groups are compared within the same target_dimension. Two groups from
        the same source_types are NOT considered independent.

        Note: EvidenceGroup objects do not carry dollar estimates directly,
        so this method returns an empty result. Use verify_valuations() for
        dollar-based comparison.
        """
        return self._build_result([], [])

    def verify_valuations(
        self,
        valuations: list[Valuation],
        groups: list[EvidenceGroup] | None = None,
    ) -> VerificationResult:
        """Verify valuations by comparing dollar estimates across source types.

        Valuations are compared within the same target_dimension. If groups are
        provided, source_types from the group are used to determine independence.
        """
        confirmations: list[SourceAgreement] = []
        contradictions: list[SourceDisagreement] = []

        group_by_id: dict[int, EvidenceGroup] = {}
        if groups:
            for g in groups:
                if g.id is not None:
                    group_by_id[g.id] = g

        # Group valuations by dimension, attaching source label
        by_dimension: dict[str, list[tuple[Valuation, str]]] = {}
        for v in valuations:
            dollar = _dollar_mid_from_valuation(v)
            if dollar is None or dollar <= 0:
                continue

            group = group_by_id.get(v.group_id)
            if group:
                dim = group.target_dimension.value if isinstance(group.target_dimension, TargetDimension) else str(group.target_dimension)
                source_label = _source_label_from_group(group)
            else:
                dim = "unknown"
                source_label = _source_label_from_valuation(v)

            by_dimension.setdefault(dim, []).append((v, source_label))

        for dim, dim_vals in by_dimension.items():
            for (va, label_a), (vb, label_b) in combinations(dim_vals, 2):
                if label_a == label_b:
                    continue

                dollar_a = _dollar_mid_from_valuation(va)
                dollar_b = _dollar_mid_from_valuation(vb)
                if dollar_a is None or dollar_b is None:
                    continue

                result = _compare_pair(label_a, label_b, dollar_a, dollar_b, dim)
                if isinstance(result, SourceAgreement):
                    confirmations.append(result)
                elif isinstance(result, SourceDisagreement):
                    contradictions.append(result)

        return self._build_result(confirmations, contradictions)

    def _build_result(
        self,
        confirmations: list[SourceAgreement],
        contradictions: list[SourceDisagreement],
    ) -> VerificationResult:
        """Build a VerificationResult from collected confirmations and contradictions."""
        agreement_score = _compute_agreement_score(confirmations, contradictions)
        confidence_adj = compute_confidence_adjustment_from_counts(
            len(confirmations), len(contradictions)
        )

        return VerificationResult(
            confirmations=confirmations,
            contradictions=contradictions,
            agreement_score=agreement_score,
            confidence_adjustment=confidence_adj,
        )


# ── Scoring functions ────────────────────────────────────────────────────


def _compute_agreement_score(
    confirmations: list[SourceAgreement],
    contradictions: list[SourceDisagreement],
) -> float:
    """Compute an agreement score between 0 and 1.

    Score = confirmations / (confirmations + contradictions).
    If no comparisons are possible, returns 0.0.
    """
    total = len(confirmations) + len(contradictions)
    if total == 0:
        return 0.0
    return len(confirmations) / total


def compute_confidence_adjustment_from_counts(
    num_confirmations: int,
    num_contradictions: int,
) -> float:
    """Compute an additive confidence adjustment from confirmation/contradiction counts.

    - Each confirmation adds CONFIRMATION_BOOST (+0.03), capped at MAX_BOOST (+0.1)
    - Each contradiction subtracts CONTRADICTION_PENALTY (-0.05), floored at MIN_PENALTY (-0.2)
    - No data = 0.0 (neutral)

    Returns a value in the range [-0.2, +0.1].
    """
    if num_confirmations == 0 and num_contradictions == 0:
        return 0.0

    adjustment = 0.0
    adjustment += num_confirmations * CONFIRMATION_BOOST
    adjustment -= num_contradictions * CONTRADICTION_PENALTY

    return max(MIN_PENALTY, min(MAX_BOOST, adjustment))


def compute_confidence_adjustment(result: VerificationResult) -> float:
    """Compute a confidence adjustment from a VerificationResult.

    Convenience wrapper around compute_confidence_adjustment_from_counts.
    """
    return compute_confidence_adjustment_from_counts(
        len(result.confirmations),
        len(result.contradictions),
    )


# ── DB-backed helpers ────────────────────────────────────────────────────


def _get_dollar_mid(val) -> float | None:
    """Extract dollar_mid from a ValuationModel, falling back to average of low/high."""
    if val.dollar_mid is not None:
        return val.dollar_mid
    if val.dollar_low is not None and val.dollar_high is not None:
        return (val.dollar_low + val.dollar_high) / 2.0
    return None


def _get_source_types_for_group(group_model, session) -> list[str]:
    """Get distinct source_types for an evidence group.

    Uses the group's source_types array first. If empty, falls back to
    querying passages for their source_type.
    """
    from ai_opportunity_index.storage.models import EvidenceGroupPassageModel

    if group_model.source_types:
        return list(group_model.source_types)

    passages = session.query(EvidenceGroupPassageModel.source_type).filter(
        EvidenceGroupPassageModel.group_id == group_model.id,
        EvidenceGroupPassageModel.source_type.isnot(None),
    ).distinct().all()

    return [p.source_type for p in passages if p.source_type]


# ── Integration function ─────────────────────────────────────────────────


def verify_company_evidence(
    company_id: int,
    session,
) -> VerificationResult:
    """Load evidence groups and valuations for a company and run cross-source verification.

    Queries EvidenceGroupModel and ValuationModel directly from the database,
    groups valuations by target_dimension and source_type, compares dollar_mid
    values across source pairs, and returns a VerificationResult.

    Args:
        company_id: The company to verify.
        session: A SQLAlchemy session (sync).

    Returns:
        VerificationResult with confirmations, contradictions, agreement_score,
        and confidence_adjustment.
    """
    from ai_opportunity_index.storage.models import (
        CompanyModel,
        EvidenceGroupModel,
        EvidenceGroupPassageModel,
        ValuationModel,
    )

    # Look up company ticker
    company = session.query(CompanyModel).filter(
        CompanyModel.id == company_id
    ).first()
    ticker = company.ticker if company else ""

    # Fetch all evidence groups for this company
    groups = session.query(EvidenceGroupModel).filter(
        EvidenceGroupModel.company_id == company_id
    ).all()

    if not groups:
        result = VerificationResult(company_id=company_id, ticker=ticker)
        logger.info(
            "Verification for company %d (%s): no evidence groups found",
            company_id, ticker,
        )
        return result

    # For each group, get the FINAL stage valuation and resolve source types.
    # Structure: {dimension -> [(source_label, dollar_mid, group_id), ...]}
    DimEntry = tuple  # (source_label: str, dollar_mid: float, group_id: int)
    by_dimension: dict[str, list[DimEntry]] = defaultdict(list)

    for group in groups:
        val = session.query(ValuationModel).filter(
            ValuationModel.group_id == group.id,
            ValuationModel.stage == "final",
        ).first()

        if val is None:
            continue

        dollar = _get_dollar_mid(val)
        if dollar is None or dollar <= 0:
            continue

        # Determine the dimension key
        dim = str(group.target_dimension) if group.target_dimension else "unknown"

        # Determine source label from group's source_types
        source_types = _get_source_types_for_group(group, session)
        if source_types:
            source_label = "+".join(sorted(source_types))
        else:
            # Fall back to evidence_type from the valuation
            source_label = f"{val.evidence_type}:group_{group.id}"

        by_dimension[dim].append((source_label, dollar, group.id))

    # Compare all pairs within each dimension
    confirmations: list[SourceAgreement] = []
    contradictions: list[SourceDisagreement] = []

    for dim, entries in by_dimension.items():
        for (label_a, dollar_a, gid_a), (label_b, dollar_b, gid_b) in combinations(entries, 2):
            # Skip pairs from the same source type (not independent)
            if label_a == label_b:
                continue

            result = _compare_pair(label_a, label_b, dollar_a, dollar_b, dim)
            if isinstance(result, SourceAgreement):
                confirmations.append(result)
            elif isinstance(result, SourceDisagreement):
                contradictions.append(result)

    # Compute scores
    agreement_score = _compute_agreement_score(confirmations, contradictions)
    confidence_adj = compute_confidence_adjustment_from_counts(
        len(confirmations), len(contradictions)
    )

    verification = VerificationResult(
        company_id=company_id,
        ticker=ticker,
        confirmations=confirmations,
        contradictions=contradictions,
        agreement_score=agreement_score,
        confidence_adjustment=confidence_adj,
    )

    logger.info(
        "Verification for company %d (%s): %d groups, %d comparisons, "
        "%d confirmations, %d contradictions, agreement=%.2f, adjustment=%.3f",
        company_id,
        ticker,
        len(groups),
        len(confirmations) + len(contradictions),
        len(confirmations),
        len(contradictions),
        agreement_score,
        confidence_adj,
    )

    return verification
