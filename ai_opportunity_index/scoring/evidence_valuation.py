"""Stages 2-4: Evidence Valuation Pipeline.

Stage 2: Preliminary valuation (LLM, parallel per group)
Stage 3: Regressive final valuation (LLM, sequential per group)
Stage 4: Aggregate into dimension scores

Uses pydantic_ai agents with structured output.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
from datetime import date, datetime

from pydantic import BaseModel

from ai_opportunity_index.config import LLM_EXTRACTION_MODEL, get_google_provider
from ai_opportunity_index.domains import (
    CaptureDetails,
    EvidenceGroup,
    InvestmentDetails,
    PlanDetails,
    Valuation,
    ValuationDiscrepancy,
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
    "plan": 0.3,
    "investment": 0.7,
    "capture": 1.0,
}

# Recency decay: 0.7^years_old, floor 0.3
RECENCY_DECAY_BASE = 0.7
RECENCY_FLOOR = 0.3


# ── Pydantic output models for structured LLM responses ─────────────────


class PreliminaryOutput(BaseModel):
    evidence_type: str  # plan, investment, capture
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
    evidence_type: str
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
    return {k: v for k, v in d.items() if v is not None}


def compute_factor_score(
    specificity: float,
    magnitude: float,
    stage_weight: float,
    recency: float,
) -> float:
    """Factor score = specificity * magnitude * stage_weight * recency."""
    return specificity * magnitude * stage_weight * recency


# ── Retry config ──────────────────────────────────────────────────────────

from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)


def _is_retryable(exc: BaseException) -> bool:
    """Return True for rate-limit and transient server errors."""
    s = str(exc)
    return "429" in s or "RESOURCE_EXHAUSTED" in s or "503" in s or "500" in s


_llm_retry = retry(
    retry=retry_if_exception(_is_retryable),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=5, min=5, max=60),
    before_sleep=before_sleep_log(logger, logging.INFO),
    reraise=True,
)


async def _run_with_retry(agent, prompt):
    """Run an agent with tenacity exponential backoff on 429/5xx errors."""

    @_llm_retry
    async def _call():
        return await agent.run(prompt)

    return await _call()


# ── LLM Agent initialization ─────────────────────────────────────────────


def _get_agent(output_type):
    """Lazily create a pydantic_ai Agent for valuation."""
    from pydantic_ai import Agent
    from pydantic_ai.models.google import GoogleModel

    model = GoogleModel(LLM_EXTRACTION_MODEL, provider=get_google_provider())
    return Agent(
        model,
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
) -> PreliminaryOutput:
    """Run preliminary LLM valuation for a single evidence group."""
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

    return result.output


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

        output: PreliminaryOutput = result

        # Parse type-specific details
        plan_detail = None
        investment_detail = None
        capture_detail = None

        if output.evidence_type == "plan" and output.plan_detail:
            plan_detail = PlanDetails(**_strip_nones(output.plan_detail))
        elif output.evidence_type == "investment" and output.investment_detail:
            investment_detail = InvestmentDetails(**_strip_nones(output.investment_detail))
        elif output.evidence_type == "capture" and output.capture_detail:
            capture_detail = CaptureDetails(**_strip_nones(output.capture_detail))

        # Update group with classified evidence_type
        group.evidence_type = output.evidence_type

        val = Valuation(
            group_id=group.id,
            pipeline_run_id=pipeline_run_id,
            stage="preliminary",
            evidence_type=output.evidence_type,
            narrative=output.narrative,
            confidence=output.confidence,
            dollar_low=output.dollar_low,
            dollar_high=output.dollar_high,
            dollar_rationale=output.dollar_rationale,
            specificity=output.specificity,
            plan_detail=plan_detail,
            investment_detail=investment_detail,
            capture_detail=capture_detail,
        )

        save_valuation(val, session=session)
        preliminary_valuations.append(val)
        logger.info(
            "[%s] Preliminary: group=%d type=%s confidence=%.2f dollars=$%.0f-$%.0f specificity=%.2f",
            ticker, group.id, output.evidence_type,
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
) -> FinalOutput:
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

    return result.output


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
            output = await _final_value_group(
                group, preliminary, final_valuations, final_groups,
                company_name, ticker, sector, revenue,
                semaphore=semaphore,
            )
        except Exception as e:
            logger.warning("[%s] Final valuation failed for group %d: %s", ticker, group.id, e)
            continue

        # Parse type-specific details
        plan_detail = None
        investment_detail = None
        capture_detail = None

        if output.evidence_type == "plan" and output.plan_detail:
            plan_detail = PlanDetails(**_strip_nones(output.plan_detail))
        elif output.evidence_type == "investment" and output.investment_detail:
            investment_detail = InvestmentDetails(**_strip_nones(output.investment_detail))
        elif output.evidence_type == "capture" and output.capture_detail:
            capture_detail = CaptureDetails(**_strip_nones(output.capture_detail))

        # Compute factor score components
        dollar_mid = None
        if output.dollar_low is not None and output.dollar_high is not None:
            dollar_mid = (output.dollar_low + output.dollar_high) / 2.0

        specificity = max(0.0, min(1.0, output.specificity))
        magnitude = compute_magnitude(dollar_mid, revenue)
        stage_weight = STAGE_WEIGHTS.get(output.evidence_type, 0.5)
        recency = compute_recency(group.date_latest)
        factor_score = compute_factor_score(specificity, magnitude, stage_weight, recency)

        val = Valuation(
            group_id=group.id,
            pipeline_run_id=pipeline_run_id,
            stage="final",
            preliminary_id=preliminary.id,
            evidence_type=output.evidence_type,
            narrative=output.narrative,
            confidence=output.confidence,
            dollar_low=output.dollar_low,
            dollar_high=output.dollar_high,
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
        )

        save_valuation(val, session=session)
        final_valuations.append(val)
        final_groups.append(group)

        logger.info(
            "[%s] Final: group=%d type=%s factor_score=%.4f "
            "(spec=%.2f mag=%.4f sw=%.1f rec=%.2f) dollars=$%.0f-$%.0f%s",
            ticker, group.id, output.evidence_type, factor_score,
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
    dim_scores: dict[str, float] = {"cost": 0.0, "revenue": 0.0, "general": 0.0}
    potential_dollars: dict[str, float] = {"cost": 0.0, "revenue": 0.0, "general": 0.0}
    actual_dollars: dict[str, float] = {"cost": 0.0, "revenue": 0.0, "general": 0.0}

    for val in final_valuations:
        group = group_by_id.get(val.group_id)
        if not group:
            continue

        dim = group.target_dimension
        if dim not in dim_scores:
            dim = "general"

        # Add factor score with diminishing returns
        fs = val.factor_score or 0.0
        dim_scores[dim] += fs

        dollar_mid = val.dollar_mid or 0.0
        if val.evidence_type == "capture":
            actual_dollars[dim] += dollar_mid
        else:
            potential_dollars[dim] += dollar_mid

    # Cap dimension scores at 1.0 using diminishing returns (1 - e^(-sum))
    for dim in dim_scores:
        raw = dim_scores[dim]
        dim_scores[dim] = min(1.0, 1.0 - math.exp(-raw)) if raw > 0 else 0.0

    return {
        "cost_score": round(dim_scores["cost"], 4),
        "revenue_score": round(dim_scores["revenue"], 4),
        "general_score": round(dim_scores["general"], 4),
        "cost_potential_usd": round(potential_dollars["cost"], 2),
        "cost_actual_usd": round(actual_dollars["cost"], 2),
        "revenue_potential_usd": round(potential_dollars["revenue"], 2),
        "revenue_actual_usd": round(actual_dollars["revenue"], 2),
        "general_potential_usd": round(potential_dollars["general"], 2),
        "general_actual_usd": round(actual_dollars["general"], 2),
        "total_groups": len(final_valuations),
        "groups_by_type": {
            "plan": sum(1 for v in final_valuations if v.evidence_type == "plan"),
            "investment": sum(1 for v in final_valuations if v.evidence_type == "investment"),
            "capture": sum(1 for v in final_valuations if v.evidence_type == "capture"),
        },
    }


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
