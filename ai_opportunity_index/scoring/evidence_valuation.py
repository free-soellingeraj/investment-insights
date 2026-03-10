"""Stages 2-4: Evidence Valuation Pipeline.

Stage 2: Preliminary valuation (LLM, parallel per group)
Stage 3: Regressive final valuation (LLM, sequential per group)
Stage 4: Aggregate into dimension scores

Uses pydantic_ai agents with structured output.
"""

from __future__ import annotations

import asyncio
import logging
import math
from datetime import date, datetime

from pydantic import BaseModel

from ai_opportunity_index.config import LLM_EXTRACTION_MODEL
from ai_opportunity_index.scoring.calibration import (
    calibrate_confidence,
    check_dollar_sanity,
    source_authority_weight,
    temporal_weight,
)
from ai_opportunity_index.domains import (
    CaptureDetails,
    EvidenceGroup,
    InvestmentDetails,
    PlanDetails,
    TargetDimension,
    Valuation,
    ValuationDiscrepancy,
    ValuationEvidenceType,
    ValuationStage,
)
from ai_opportunity_index.prompts import load_prompt
from ai_opportunity_index.storage.db import (
    get_final_valuations_for_company,
    save_evidence_group,
    save_valuation,
    save_valuation_discrepancy,
)

logger = logging.getLogger(__name__)

# ── Stage weight constants ────────────────────────────────────────────────
STAGE_WEIGHTS = {
    ValuationEvidenceType.PLAN: 0.3,
    ValuationEvidenceType.INVESTMENT: 0.7,
    ValuationEvidenceType.CAPTURE: 1.0,
}

# Recency decay: 0.7^years_old, floor 0.3
RECENCY_DECAY_BASE = 0.7
RECENCY_FLOOR = 0.3


# ── Pydantic output models for structured LLM responses ─────────────────


class PreliminaryOutput(BaseModel):
    evidence_type: str  # plan, investment, capture — normalized to lowercase
    narrative: str
    confidence: float
    dollar_low: float
    dollar_high: float
    dollar_rationale: str
    specificity: float
    plan_detail: dict | None = None
    investment_detail: dict | None = None
    capture_detail: dict | None = None


class DiscrepancyInfo(BaseModel):
    prior_group_index: int
    description: str
    resolution: str
    resolution_method: str
    trusted_current: bool


class FinalOutput(BaseModel):
    evidence_type: str  # plan, investment, capture — normalized to lowercase
    narrative: str
    confidence: float
    dollar_low: float
    dollar_high: float
    dollar_rationale: str
    specificity: float
    adjusted_from_preliminary: bool = False
    adjustment_reason: str | None = None
    plan_detail: dict | None = None
    investment_detail: dict | None = None
    capture_detail: dict | None = None
    discrepancies: list[DiscrepancyInfo] = []


def _map_source_type_to_calibration(source_type: str) -> str:
    """Map evidence source types to calibration curve keys."""
    _SOURCE_TYPE_MAP = {
        "filing": "sec_filing",
        "sec_filing": "sec_filing",
        "news": "news",
        "github": "github",
    }
    return _SOURCE_TYPE_MAP.get(source_type, "llm")


def _dominant_source_type(group: EvidenceGroup) -> str:
    """Return the most common source type in a group, mapped for calibration."""
    from collections import Counter
    types = [p.source_type or "unknown" for p in group.passages]
    if not types:
        return "llm"
    dominant = Counter(types).most_common(1)[0][0]
    return _map_source_type_to_calibration(dominant)


def _normalize_evidence_type(raw: str) -> ValuationEvidenceType:
    """Normalize LLM evidence_type output to ValuationEvidenceType enum.

    LLMs may return uppercase ('INVESTMENT'), title case ('Plan'), or lowercase
    ('capture'). This normalizes to the expected enum value.
    """
    lowered = raw.strip().lower()
    try:
        return ValuationEvidenceType(lowered)
    except ValueError:
        logger.warning("Unknown evidence_type from LLM: %r, defaulting to 'investment'", raw)
        return ValuationEvidenceType.INVESTMENT


def _normalize_horizon_shape(d: dict | None) -> dict:
    """Normalize horizon_shape in a detail dict from LLM output.

    LLMs may return uppercase enum names like 'LINEAR_RAMP' instead of
    'linear_ramp'. Normalize to lowercase for HorizonShape enum compatibility.
    """
    if not d or "horizon_shape" not in d:
        return d or {}
    val = d.get("horizon_shape")
    if isinstance(val, str):
        d["horizon_shape"] = val.strip().lower()
    return d


# ── Factor score computation ──────────────────────────────────────────────


def compute_recency(evidence_date: date | None, reference_date: date | None = None) -> float:
    """Compute recency decay: 0.7^years_old, floor 0.3."""
    if evidence_date is None:
        return RECENCY_FLOOR
    ref = reference_date or date.today()
    years_old = max(0.0, (ref - evidence_date).days / 365.25)
    return max(RECENCY_FLOOR, RECENCY_DECAY_BASE ** years_old)


def compute_magnitude(dollar_mid: float | None, company_revenue: float | None) -> float:
    """Compute magnitude: dollar_mid / company_revenue, capped at 1.0."""
    if not dollar_mid or not company_revenue or company_revenue <= 0:
        return 0.0
    return min(1.0, abs(dollar_mid) / company_revenue)


def _strip_nones(d: dict | None) -> dict:
    """Strip None values from a dict so Pydantic defaults kick in.

    LLMs sometimes return null for optional fields that have non-None defaults
    in the Pydantic model (e.g. deployment_scope: str = ""). Passing None
    triggers a validation error, so we drop those keys.
    """
    if not d:
        return {}
    cleaned = {k: v for k, v in d.items() if v is not None}
    # Truncate string fields that map to limited DB columns
    _STR_LIMITS = {
        "timeframe": 50,
        "vendor_partner": 200,
        "metric_name": 200,
    }
    for field, limit in _STR_LIMITS.items():
        if field in cleaned and isinstance(cleaned[field], str) and len(cleaned[field]) > limit:
            cleaned[field] = cleaned[field][:limit]
    return cleaned


# Fields on CaptureDetails that are typed str but LLMs sometimes return as float/int
_CAPTURE_STR_FIELDS = {"metric_name", "metric_value_before", "metric_value_after", "metric_delta", "measurement_period"}


def _parse_dollar_string(val: str) -> float | None:
    """Try to parse a dollar string like '$10,000,000' or '$10B - $25B' into a float.

    For ranges, takes the midpoint. Returns None if unparseable.
    """
    import re
    # Strip whitespace
    val = val.strip()
    # Handle range: take first number
    parts = re.split(r'\s*[-–—]\s*', val)
    nums = []
    for part in parts:
        # Remove $, commas, whitespace
        cleaned = re.sub(r'[$,\s]', '', part)
        try:
            nums.append(float(cleaned))
        except ValueError:
            pass
    if not nums:
        return None
    # Midpoint for ranges, single value otherwise
    return sum(nums) / len(nums)


def _coerce_capture_detail(d: dict | None) -> dict:
    """Strip nones and coerce LLM outputs for CaptureDetails fields.

    Handles two known LLM issues:
    - str fields receiving float/int values (metric_value_before, etc.)
    - float field (measured_dollar_impact) receiving formatted strings like '$10,000,000,000'
    """
    cleaned = _strip_nones(d)
    for key in _CAPTURE_STR_FIELDS:
        if key in cleaned and not isinstance(cleaned[key], str):
            cleaned[key] = str(cleaned[key])
    # measured_dollar_impact should be float but LLMs sometimes return dollar strings
    if "measured_dollar_impact" in cleaned and isinstance(cleaned["measured_dollar_impact"], str):
        parsed = _parse_dollar_string(cleaned["measured_dollar_impact"])
        if parsed is not None:
            cleaned["measured_dollar_impact"] = parsed
        else:
            del cleaned["measured_dollar_impact"]  # drop unparseable, let default (None) apply
    return cleaned


def compute_factor_score(
    specificity: float,
    magnitude: float,
    stage_weight: float,
    recency: float,
    authority_weight: float = 1.0,
) -> float:
    """Factor score = specificity * magnitude * stage_weight * recency * authority_weight."""
    return specificity * magnitude * stage_weight * recency * authority_weight


# ── Retry config ──────────────────────────────────────────────────────────

from ai_opportunity_index.llm_backend import run_agent_with_retry as _run_with_retry  # noqa: E402


# ── LLM Agent initialization ─────────────────────────────────────────────


def _get_agent(output_type):
    """Lazily create an LLM Agent for valuation."""
    from ai_opportunity_index.llm_backend import get_agent

    return get_agent(
        output_type=output_type,
        system_prompt=(
            "You are a senior AI investment analyst. You evaluate corporate AI evidence "
            "and produce structured valuations. Be analytical and precise. Return valid JSON "
            "matching the requested schema."
        ),
    )


# ── Stage 2: Preliminary Valuation ───────────────────────────────────────


async def _preliminary_value_group(
    group: EvidenceGroup,
    company_name: str,
    ticker: str,
    sector: str,
    revenue: float,
    semaphore: asyncio.Semaphore | None = None,
) -> tuple[PreliminaryOutput, int, int]:
    """Run preliminary LLM valuation for a single evidence group.

    Returns (output, input_tokens, output_tokens).
    """
    revenue_str = f"${revenue:,.0f}" if revenue else "Unknown"

    passages_data = [
        {
            "source_type": p.source_type or "unknown",
            "source_filename": p.source_filename or "unknown",
            "source_date": str(p.source_date) if p.source_date else "unknown",
            "capture_stage": p.capture_stage or "unknown",
            "confidence": p.confidence,
            "passage_text": p.passage_text[:1000],
        }
        for p in group.passages
    ]

    prompt = load_prompt(
        "value_preliminary_evidence",
        company_name=company_name,
        ticker=ticker,
        sector=sector or "Unknown",
        revenue_str=revenue_str,
        target_dimension=group.target_dimension,
        passage_count=len(group.passages),
        passages=passages_data,
    )

    agent = _get_agent(PreliminaryOutput)

    if semaphore:
        async with semaphore:
            result = await _run_with_retry(agent, prompt)
    else:
        result = await _run_with_retry(agent, prompt)

    usage = result.usage()
    return result.output, usage.input_tokens or 0, usage.output_tokens or 0


async def run_preliminary_valuations(
    groups: list[EvidenceGroup],
    company_name: str,
    ticker: str,
    sector: str,
    revenue: float,
    pipeline_run_id: int | None = None,
    semaphore: asyncio.Semaphore | None = None,
    session=None,
) -> list[Valuation]:
    """Stage 2: Run preliminary valuations in parallel for all groups.

    Saves groups to DB, runs LLM calls, saves valuations.
    Returns list of preliminary Valuation domain objects.
    """
    preliminary_valuations: list[Valuation] = []

    # Save groups to DB first
    for group in groups:
        save_evidence_group(group, session=session)

    # Run LLM calls in parallel
    tasks = [
        _preliminary_value_group(
            group, company_name, ticker, sector, revenue,
            semaphore=semaphore,
        )
        for group in groups
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for group, result in zip(groups, results):
        if isinstance(result, Exception):
            logger.warning(
                "[%s] Preliminary valuation failed for group %s: %s",
                ticker, group.id, result,
            )
            continue

        output, in_tokens, out_tokens = result
        ev_type = _normalize_evidence_type(output.evidence_type)

        # Calibrate LLM confidence using actual source type
        raw_confidence = output.confidence
        calibration_source = _dominant_source_type(group)
        calibrated_confidence = calibrate_confidence(raw_confidence, calibration_source)
        logger.debug("[%s] Calibrated confidence: %.2f -> %.2f (source=%s)", ticker, raw_confidence, calibrated_confidence, calibration_source)

        # Parse type-specific details
        plan_detail = None
        investment_detail = None
        capture_detail = None

        if ev_type == ValuationEvidenceType.PLAN and output.plan_detail:
            plan_detail = PlanDetails(**_normalize_horizon_shape(_strip_nones(output.plan_detail)))
        elif ev_type == ValuationEvidenceType.INVESTMENT and output.investment_detail:
            investment_detail = InvestmentDetails(**_normalize_horizon_shape(_strip_nones(output.investment_detail)))
        elif ev_type == ValuationEvidenceType.CAPTURE and output.capture_detail:
            capture_detail = CaptureDetails(**_normalize_horizon_shape(_coerce_capture_detail(output.capture_detail)))

        # Update group with classified evidence_type
        group.evidence_type = ev_type

        # Ensure low <= high numerically (LLMs sometimes reverse for negatives)
        prelim_dollar_low = output.dollar_low
        prelim_dollar_high = output.dollar_high
        if prelim_dollar_low is not None and prelim_dollar_high is not None and prelim_dollar_low > prelim_dollar_high:
            prelim_dollar_low, prelim_dollar_high = prelim_dollar_high, prelim_dollar_low

        val = Valuation(
            group_id=group.id,
            pipeline_run_id=pipeline_run_id,
            stage=ValuationStage.PRELIMINARY,
            evidence_type=ev_type,
            narrative=output.narrative,
            confidence=calibrated_confidence,
            dollar_low=prelim_dollar_low,
            dollar_high=prelim_dollar_high,
            dollar_rationale=output.dollar_rationale,
            specificity=output.specificity,
            plan_detail=plan_detail,
            investment_detail=investment_detail,
            capture_detail=capture_detail,
            input_tokens=in_tokens,
            output_tokens=out_tokens,
            model_name=LLM_EXTRACTION_MODEL,
        )

        save_valuation(val, session=session)
        preliminary_valuations.append(val)
        logger.info(
            "[%s] Preliminary: group=%d type=%s confidence=%.2f dollars=$%.0f-$%.0f specificity=%.2f",
            ticker, group.id, ev_type.value,
            output.confidence, output.dollar_low, output.dollar_high, output.specificity,
        )

    return preliminary_valuations


# ── Stage 3: Regressive Final Valuation ──────────────────────────────────


async def _final_value_group(
    group: EvidenceGroup,
    preliminary: Valuation,
    prior_finals: list[Valuation],
    prior_groups: list[EvidenceGroup],
    company_name: str,
    ticker: str,
    sector: str,
    revenue: float,
    semaphore: asyncio.Semaphore | None = None,
) -> tuple[FinalOutput, int, int]:
    """Run final regressive LLM valuation for a single group."""
    revenue_str = f"${revenue:,.0f}" if revenue else "Unknown"

    passages_data = [
        {
            "source_type": p.source_type or "unknown",
            "source_date": str(p.source_date) if p.source_date else "undated",
            "passage_text": p.passage_text[:800],
        }
        for p in group.passages
    ]

    # Condense prior valuations for context
    prior_context = []
    for pv, pg in zip(prior_finals, prior_groups):
        prior_context.append({
            "evidence_type": pv.evidence_type,
            "target_dimension": pg.target_dimension,
            "narrative": pv.narrative[:300],
            "dollar_low": pv.dollar_low,
            "dollar_high": pv.dollar_high,
            "dollar_mid": pv.dollar_mid,
            "factor_score": pv.factor_score,
            "specificity": pv.specificity,
        })

    prompt = load_prompt(
        "value_final_evidence",
        company_name=company_name,
        ticker=ticker,
        sector=sector or "Unknown",
        revenue_str=revenue_str,
        target_dimension=group.target_dimension,
        preliminary_type=preliminary.evidence_type,
        preliminary_narrative=preliminary.narrative[:500],
        preliminary_dollar_low=f"{preliminary.dollar_low or 0:,.0f}",
        preliminary_dollar_high=f"{preliminary.dollar_high or 0:,.0f}",
        preliminary_specificity=f"{preliminary.specificity or 0:.2f}",
        passages=passages_data,
        prior_count=len(prior_finals),
        prior_valuations=prior_context,
    )

    agent = _get_agent(FinalOutput)

    if semaphore:
        async with semaphore:
            result = await _run_with_retry(agent, prompt)
    else:
        result = await _run_with_retry(agent, prompt)

    usage = result.usage()
    return result.output, usage.input_tokens or 0, usage.output_tokens or 0


async def run_final_valuations(
    groups: list[EvidenceGroup],
    preliminary_valuations: list[Valuation],
    company_id: int,
    company_name: str,
    ticker: str,
    sector: str,
    revenue: float,
    pipeline_run_id: int | None = None,
    semaphore: asyncio.Semaphore | None = None,
    session=None,
) -> list[Valuation]:
    """Stage 3: Run regressive final valuations sequentially.

    Each group sees all prior final valuations for context.
    Returns list of final Valuation domain objects with factor scores.
    """
    final_valuations: list[Valuation] = []
    final_groups: list[EvidenceGroup] = []

    # Build preliminary lookup: group_id → preliminary valuation
    prelim_by_group = {v.group_id: v for v in preliminary_valuations}

    for group in groups:
        preliminary = prelim_by_group.get(group.id)
        if preliminary is None:
            logger.warning("[%s] No preliminary valuation for group %d, skipping", ticker, group.id)
            continue

        try:
            output, in_tokens, out_tokens = await _final_value_group(
                group, preliminary, final_valuations, final_groups,
                company_name, ticker, sector, revenue,
                semaphore=semaphore,
            )
        except Exception as e:
            logger.warning("[%s] Final valuation failed for group %d: %s", ticker, group.id, e)
            continue

        # Normalize LLM output
        ev_type = _normalize_evidence_type(output.evidence_type)

        # Calibrate LLM confidence using actual source type
        raw_confidence = output.confidence
        calibration_source = _dominant_source_type(group)
        calibrated_confidence = calibrate_confidence(raw_confidence, calibration_source)
        logger.debug("[%s] Calibrated confidence: %.2f -> %.2f (source=%s)", ticker, raw_confidence, calibrated_confidence, calibration_source)

        # Parse type-specific details
        plan_detail = None
        investment_detail = None
        capture_detail = None

        if ev_type == ValuationEvidenceType.PLAN and output.plan_detail:
            plan_detail = PlanDetails(**_normalize_horizon_shape(_strip_nones(output.plan_detail)))
        elif ev_type == ValuationEvidenceType.INVESTMENT and output.investment_detail:
            investment_detail = InvestmentDetails(**_normalize_horizon_shape(_strip_nones(output.investment_detail)))
        elif ev_type == ValuationEvidenceType.CAPTURE and output.capture_detail:
            capture_detail = CaptureDetails(**_normalize_horizon_shape(_coerce_capture_detail(output.capture_detail)))

        # Compute factor score components
        dollar_low = output.dollar_low
        dollar_high = output.dollar_high
        # Ensure low <= high numerically (LLMs sometimes reverse for negatives)
        if dollar_low is not None and dollar_high is not None and dollar_low > dollar_high:
            dollar_low, dollar_high = dollar_high, dollar_low

        # Apply dollar sanity to low and high individually BEFORE computing mid
        if dollar_low is not None and dollar_low > 0:
            dollar_low, low_warnings = check_dollar_sanity(dollar_low, revenue, None)
            for warning in low_warnings:
                logger.warning("[%s] Dollar sanity low (group=%d): %s", ticker, group.id, warning)
        if dollar_high is not None and dollar_high > 0:
            dollar_high, high_warnings = check_dollar_sanity(dollar_high, revenue, None)
            for warning in high_warnings:
                logger.warning("[%s] Dollar sanity high (group=%d): %s", ticker, group.id, warning)

        dollar_mid = None
        if dollar_low is not None and dollar_high is not None:
            dollar_mid = (dollar_low + dollar_high) / 2.0

        # Compute authority weight from the evidence group's source types
        authority_weights = []
        for passage in group.passages:
            src_type = passage.source_type or "unknown"
            src_authority = passage.source_authority.value if passage.source_authority else None
            authority_weights.append(source_authority_weight(src_type, src_authority))
        auth_weight = max(authority_weights) if authority_weights else 1.0

        specificity = max(0.0, min(1.0, output.specificity))
        magnitude = compute_magnitude(dollar_mid, revenue)
        stage_weight = STAGE_WEIGHTS.get(ev_type, 0.5)
        # Use source-specific temporal decay instead of flat recency
        if group.date_latest is not None:
            days_old = max(0, (date.today() - group.date_latest).days)
        else:
            days_old = 180  # default age for undated groups
        recency = temporal_weight(days_old, calibration_source)
        factor_score = compute_factor_score(specificity, magnitude, stage_weight, recency, auth_weight)

        val = Valuation(
            group_id=group.id,
            pipeline_run_id=pipeline_run_id,
            stage=ValuationStage.FINAL,
            preliminary_id=preliminary.id,
            evidence_type=ev_type,
            narrative=output.narrative,
            confidence=calibrated_confidence,
            dollar_low=dollar_low,
            dollar_high=dollar_high,
            dollar_mid=dollar_mid,
            dollar_rationale=output.dollar_rationale,
            specificity=specificity,
            magnitude=magnitude,
            stage_weight=stage_weight,
            recency=recency,
            factor_score=factor_score,
            adjusted_from_preliminary=output.adjusted_from_preliminary,
            adjustment_reason=output.adjustment_reason,
            prior_groups_seen=len(final_valuations),
            plan_detail=plan_detail,
            investment_detail=investment_detail,
            capture_detail=capture_detail,
            input_tokens=in_tokens,
            output_tokens=out_tokens,
            model_name=LLM_EXTRACTION_MODEL,
        )

        save_valuation(val, session=session)
        final_valuations.append(val)
        final_groups.append(group)

        logger.info(
            "[%s] Final: group=%d type=%s factor_score=%.4f "
            "(spec=%.2f mag=%.4f sw=%.1f rec=%.2f) dollars=$%.0f-$%.0f%s",
            ticker, group.id, ev_type.value, factor_score,
            specificity, magnitude, stage_weight, recency,
            output.dollar_low, output.dollar_high,
            " [ADJUSTED]" if output.adjusted_from_preliminary else "",
        )

        # Save discrepancies
        for disc_info in output.discrepancies:
            prior_idx = disc_info.prior_group_index - 1  # 1-indexed in prompt
            if 0 <= prior_idx < len(final_groups):
                prior_group = final_groups[prior_idx]
                disc = ValuationDiscrepancy(
                    company_id=company_id,
                    pipeline_run_id=pipeline_run_id,
                    group_id_a=group.id,
                    group_id_b=prior_group.id,
                    description=disc_info.description,
                    resolution=disc_info.resolution,
                    resolution_method=disc_info.resolution_method,
                    trusted_group_id=group.id if disc_info.trusted_current else prior_group.id,
                )
                save_valuation_discrepancy(disc, session=session)

    return final_valuations


# ── Stage 4: Aggregate ────────────────────────────────────────────────────


def aggregate_valuations(
    final_valuations: list[Valuation],
    groups: list[EvidenceGroup],
) -> dict:
    """Stage 4: Aggregate final valuations into per-dimension scores.

    Returns dict with:
    - per-dimension factor scores (capped at 1.0)
    - potential_dollars (plan + investment)
    - actual_dollars (capture only)
    - all evidence for ClassifiedScorerOutput integration
    """
    group_by_id = {g.id: g for g in groups}

    # Per-dimension aggregation
    dim_scores: dict[str, float] = {TargetDimension.COST: 0.0, TargetDimension.REVENUE: 0.0, TargetDimension.GENERAL: 0.0}
    potential_dollars: dict[str, float] = {TargetDimension.COST: 0.0, TargetDimension.REVENUE: 0.0, TargetDimension.GENERAL: 0.0}
    actual_dollars: dict[str, float] = {TargetDimension.COST: 0.0, TargetDimension.REVENUE: 0.0, TargetDimension.GENERAL: 0.0}

    for val in final_valuations:
        group = group_by_id.get(val.group_id)
        if not group:
            continue

        dim = group.target_dimension
        if dim not in dim_scores:
            dim = TargetDimension.GENERAL

        # Add factor score with diminishing returns
        fs = val.factor_score or 0.0
        dim_scores[dim] += fs

        dollar_mid = val.dollar_mid or 0.0
        if val.evidence_type == ValuationEvidenceType.CAPTURE:
            actual_dollars[dim] += dollar_mid
        else:
            potential_dollars[dim] += dollar_mid

    # Cap dimension scores at 1.0 using diminishing returns (1 - e^(-sum))
    for dim in dim_scores:
        raw = dim_scores[dim]
        dim_scores[dim] = min(1.0, 1.0 - math.exp(-raw)) if raw > 0 else 0.0

    # Collect the IDs of evidence groups and valuations that contributed
    evidence_group_ids = sorted({
        val.group_id for val in final_valuations
        if val.group_id is not None and val.group_id in group_by_id
    })
    valuation_ids = sorted({
        val.id for val in final_valuations
        if val.id is not None
    })

    result = {
        "cost_score": round(dim_scores[TargetDimension.COST], 4),
        "revenue_score": round(dim_scores[TargetDimension.REVENUE], 4),
        "general_score": round(dim_scores[TargetDimension.GENERAL], 4),
        "cost_potential_usd": round(potential_dollars[TargetDimension.COST], 2),
        "cost_actual_usd": round(actual_dollars[TargetDimension.COST], 2),
        "revenue_potential_usd": round(potential_dollars[TargetDimension.REVENUE], 2),
        "revenue_actual_usd": round(actual_dollars[TargetDimension.REVENUE], 2),
        "general_potential_usd": round(potential_dollars[TargetDimension.GENERAL], 2),
        "general_actual_usd": round(actual_dollars[TargetDimension.GENERAL], 2),
        "total_groups": len(final_valuations),
        "groups_by_type": {
            "plan": sum(1 for v in final_valuations if v.evidence_type == ValuationEvidenceType.PLAN),
            "investment": sum(1 for v in final_valuations if v.evidence_type == ValuationEvidenceType.INVESTMENT),
            "capture": sum(1 for v in final_valuations if v.evidence_type == ValuationEvidenceType.CAPTURE),
        },
        "evidence_group_ids": evidence_group_ids,
        "valuation_ids": valuation_ids,
    }

    # Apply cross-source verification adjustment if multiple source types present
    all_source_types: set[str] = set()
    for val in final_valuations:
        group = group_by_id.get(val.group_id)
        if group:
            all_source_types.update(group.source_types)

    if len(all_source_types) >= 2 and len(final_valuations) >= 2:
        try:
            from ai_opportunity_index.fact_graph.scoring_integration import apply_verification_adjustment
            from ai_opportunity_index.fact_graph.verification import verify_company_evidence
            from ai_opportunity_index.storage.db import get_session

            # All groups belong to the same company
            company_id = next(
                (group_by_id[v.group_id].company_id for v in final_valuations if v.group_id in group_by_id),
                None,
            )
            if company_id is not None:
                session = get_session()
                try:
                    verification_result = verify_company_evidence(company_id, session)
                    result = apply_verification_adjustment(result, verification_result)
                    logger.info(
                        "Cross-source verification applied: %d source types, agreement=%.2f",
                        len(all_source_types),
                        verification_result.agreement_score,
                    )
                except Exception as e:
                    logger.debug("Cross-source verification skipped: %s", e)
                finally:
                    session.close()
        except ImportError:
            logger.debug("Cross-source verification module not available")

    return result


# ── Full Pipeline ─────────────────────────────────────────────────────────


async def value_evidence_for_company(
    ticker: str,
    company_id: int,
    company_name: str,
    sector: str,
    revenue: float,
    pipeline_run_id: int | None = None,
    llm_semaphore: asyncio.Semaphore | None = None,
    session=None,
) -> dict | None:
    """Run the full evidence valuation pipeline for a single company.

    Stages: munge → preliminary → final → aggregate.
    Returns aggregated scores dict, or None if no evidence.
    """
    from ai_opportunity_index.scoring.evidence_munger import munge_evidence
    from ai_opportunity_index.storage.db import delete_evidence_groups_for_company

    # Clear previous valuation data for this company (idempotent re-run)
    delete_evidence_groups_for_company(company_id, pipeline_run_id=pipeline_run_id, session=session)

    # Stage 1: Munge
    groups = munge_evidence(ticker, company_id, pipeline_run_id=pipeline_run_id)
    if not groups:
        return None

    # Stage 2: Preliminary (parallel)
    preliminary = await run_preliminary_valuations(
        groups, company_name, ticker, sector, revenue,
        pipeline_run_id=pipeline_run_id,
        semaphore=llm_semaphore,
        session=session,
    )
    if not preliminary:
        return None

    # Stage 3: Final (sequential/regressive)
    finals = await run_final_valuations(
        groups, preliminary,
        company_id, company_name, ticker, sector, revenue,
        pipeline_run_id=pipeline_run_id,
        semaphore=llm_semaphore,
        session=session,
    )

    # Stage 4: Aggregate
    result = aggregate_valuations(finals, groups)

    logger.info(
        "[%s] Valuation complete: %d groups, cost=%.4f rev=%.4f gen=%.4f "
        "potential=$%.0f actual=$%.0f",
        ticker, result["total_groups"],
        result["cost_score"], result["revenue_score"], result["general_score"],
        sum(result[k] for k in ("cost_potential_usd", "revenue_potential_usd", "general_potential_usd")),
        sum(result[k] for k in ("cost_actual_usd", "revenue_actual_usd", "general_actual_usd")),
    )

    return result
