#!/usr/bin/env python3
"""Step 3: Run the scoring pipeline on all companies.

Computes AI Opportunity and AI Capture scores using the 4-value framework
(cost/revenue x opportunity/capture), then combines them into the final index.
Saves individual Evidence rows and unified CompanyScore records with a
PipelineRun audit trail.
"""

import argparse
import asyncio
import logging
import sys
import uuid
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import select

from ai_opportunity_index.config import RAW_DIR, CAPTURE_WEIGHTS, OPPORTUNITY_WEIGHTS, SIGNAL_STRENGTH_HIGH, SIGNAL_STRENGTH_MEDIUM
from ai_opportunity_index.data.filing_extraction import _strip_xbrl_tags
from ai_opportunity_index.data.industry_mappings import sic_to_soc_groups
from ai_opportunity_index.domains import AIOpportunityEvidence, CompanyScore, PipelineRun, PipelineSubtask, PipelineTask, ScoreChange
from ai_opportunity_index.scoring.pipeline.runner import ScoringPipeline
from ai_opportunity_index.scoring.ai_capture import compute_capture_scores, flag_capture_discrepancies
from ai_opportunity_index.scoring.ai_opportunity import compute_opportunity_score
from ai_opportunity_index.scoring.ai_realization import compute_realization_score, flag_discrepancies
from ai_opportunity_index.scoring.composite import compute_index, compute_index_4v
from ai_opportunity_index.scoring.evidence_classification import ClassifiedScorerOutput
from ai_opportunity_index.scoring.realization.analyst_scorer import score_analyst_classified
from ai_opportunity_index.scoring.realization.filing_nlp import score_filing_classified, score_filing_classified_async
from ai_opportunity_index.scoring.realization.github_scorer import score_github_classified
from ai_opportunity_index.scoring.realization.product_analysis import analyze_company_products_classified, analyze_company_products_classified_async
from ai_opportunity_index.scoring.realization.web_signals import score_web_enrichment_classified
from ai_opportunity_index.storage.db import (
    complete_pipeline_run,
    create_pipeline_run,
    get_evidence_groups_for_company,
    get_final_valuations_for_company,
    get_latest_financials,
    get_latest_score,
    get_session,
    init_db,
    refresh_latest_scores_view,
    save_company_score,
    save_evidence_batch,
    save_score_change,
)
from ai_opportunity_index.storage.models import CompanyModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

EXTRACTED_FILINGS_DIR = RAW_DIR / "extracted_filings"
EXTRACTED_NEWS_DIR = RAW_DIR / "extracted_news"

# ── Import classification enums for cache readers ─────────────────────────
from ai_opportunity_index.scoring.evidence_classification import (
    CaptureStage,
    ClassifiedEvidence,
    TargetDimension,
)

_TARGET_MAP = {
    "cost": TargetDimension.COST,
    "revenue": TargetDimension.REVENUE,
    "general": TargetDimension.GENERAL,
}

_STAGE_MAP = {
    "planned": CaptureStage.PLANNED,
    "invested": CaptureStage.INVESTED,
    "realized": CaptureStage.REALIZED,
}


def _build_scorer_from_valuations(
    company_id: int,
    source_label: str = "valuation",
) -> ClassifiedScorerOutput | None:
    """Build a ClassifiedScorerOutput from final valuations in the DB.

    Uses the evidence valuation pipeline's factor scores instead of
    naive confidence summing.
    """
    from ai_opportunity_index.scoring.evidence_valuation import aggregate_valuations

    final_vals = get_final_valuations_for_company(company_id)
    if not final_vals:
        return None

    groups = get_evidence_groups_for_company(company_id)
    if not groups:
        return None

    agg = aggregate_valuations(final_vals, groups)

    # Map evidence type to capture stage for ClassifiedEvidence
    _type_to_stage = {
        "plan": CaptureStage.PLANNED,
        "investment": CaptureStage.INVESTED,
        "capture": CaptureStage.REALIZED,
    }

    group_by_id = {g.id: g for g in groups}
    evidence_items: list[ClassifiedEvidence] = []
    for val in final_vals:
        group = group_by_id.get(val.group_id)
        if not group:
            continue
        target = _TARGET_MAP.get(group.target_dimension, TargetDimension.GENERAL)
        stage = _type_to_stage.get(val.evidence_type, CaptureStage.INVESTED)
        evidence_items.append(ClassifiedEvidence(
            source_type=source_label,
            target=target,
            stage=stage,
            raw_score=val.factor_score or 0.0,
            description=val.narrative[:200],
            source_excerpt=(group.representative_text or "")[:500],
            metadata={
                "evidence_type": val.evidence_type,
                "dollar_mid": val.dollar_mid,
                "specificity": val.specificity,
                "factor_score": val.factor_score,
            },
        ))

    return ClassifiedScorerOutput(
        overall_score=min(1.0, agg["cost_score"] + agg["revenue_score"] + agg["general_score"]),
        cost_capture_score=agg["cost_score"],
        revenue_capture_score=agg["revenue_score"],
        general_investment_score=agg["general_score"],
        evidence_items=evidence_items,
        raw_details={
            "method": "evidence_valuation",
            "total_groups": agg["total_groups"],
            "groups_by_type": agg["groups_by_type"],
            "cost_potential_usd": agg["cost_potential_usd"],
            "cost_actual_usd": agg["cost_actual_usd"],
            "revenue_potential_usd": agg["revenue_potential_usd"],
            "revenue_actual_usd": agg["revenue_actual_usd"],
        },
    )


def _read_filing_extraction_cache(ticker: str) -> ClassifiedScorerOutput | None:
    """Read pre-extracted filing data from cache and convert to ClassifiedScorerOutput."""
    import json
    cache_path = EXTRACTED_FILINGS_DIR / f"{ticker.upper()}.json"
    if not cache_path.exists():
        return None

    try:
        data = json.loads(cache_path.read_text())
    except Exception:
        return None

    filings = data.get("filings", [])
    if not filings:
        return None

    evidence_items: list[ClassifiedEvidence] = []
    cost_score = 0.0
    revenue_score = 0.0
    general_score = 0.0

    for filing in filings:
        for p in filing.get("passages", []):
            target = _TARGET_MAP.get(p.get("target_dimension", "general"), TargetDimension.GENERAL)
            stage = _STAGE_MAP.get(p.get("capture_stage", "invested"), CaptureStage.INVESTED)
            confidence = max(0.0, min(1.0, p.get("confidence", 0.0)))

            if target == TargetDimension.COST:
                cost_score += confidence
            elif target == TargetDimension.REVENUE:
                revenue_score += confidence
            else:
                general_score += confidence

            evidence_items.append(ClassifiedEvidence(
                source_type="filing_nlp",
                target=target,
                stage=stage,
                raw_score=confidence,
                description=(p.get("reasoning", "") or "")[:200] or "LLM-extracted filing evidence",
                source_excerpt=(p.get("passage_text", "") or "")[:500],
                metadata={"method": "llm", "reasoning": p.get("reasoning", "")},
            ))

    cost_score = round(min(1.0, cost_score), 4)
    revenue_score = round(min(1.0, revenue_score), 4)
    general_score = round(min(1.0, general_score), 4)
    overall = round(min(1.0, cost_score + revenue_score + general_score), 4)

    return ClassifiedScorerOutput(
        overall_score=overall,
        cost_capture_score=cost_score,
        revenue_capture_score=revenue_score,
        general_investment_score=general_score,
        evidence_items=evidence_items,
        raw_details={"method": "cached_extraction", "passages_extracted": len(evidence_items)},
    )


def _read_news_extraction_cache(ticker: str, company_name: str = "") -> ClassifiedScorerOutput | None:
    """Read pre-extracted news data from cache and convert to ClassifiedScorerOutput."""
    import json
    cache_path = EXTRACTED_NEWS_DIR / f"{ticker.upper()}.json"
    if not cache_path.exists():
        return None

    try:
        data = json.loads(cache_path.read_text())
    except Exception:
        return None

    articles = data.get("articles", [])
    if not articles:
        return None

    evidence_items: list[ClassifiedEvidence] = []
    cost_score = 0.0
    revenue_score = 0.0
    general_score = 0.0

    for article in articles:
        title = article.get("title", "")
        url = article.get("url", "")
        for p in article.get("passages", []):
            target = _TARGET_MAP.get(p.get("target_dimension", "general"), TargetDimension.GENERAL)
            stage = _STAGE_MAP.get(p.get("capture_stage", "invested"), CaptureStage.INVESTED)
            confidence = max(0.0, min(1.0, p.get("confidence", 0.0)))

            if target == TargetDimension.COST:
                cost_score += confidence
            elif target == TargetDimension.REVENUE:
                revenue_score += confidence
            else:
                general_score += confidence

            evidence_items.append(ClassifiedEvidence(
                source_type="product",
                target=target,
                stage=stage,
                raw_score=confidence,
                description=title[:200] if title else "News article",
                source_excerpt=(p.get("passage_text", "") or "")[:500],
                metadata={
                    "method": "llm",
                    "reasoning": p.get("reasoning", ""),
                    "url": url,
                },
            ))

    cost_score = round(min(1.0, cost_score), 4)
    revenue_score = round(min(1.0, revenue_score), 4)
    general_score = round(min(1.0, general_score), 4)
    overall = round(min(1.0, cost_score + revenue_score + general_score), 4)

    return ClassifiedScorerOutput(
        overall_score=overall,
        cost_capture_score=cost_score,
        revenue_capture_score=revenue_score,
        general_investment_score=general_score,
        evidence_items=evidence_items[:20],
        raw_details={
            "method": "cached_extraction",
            "articles_extracted": len(articles),
            "passages_extracted": len(evidence_items),
        },
    )


def build_evidence_items(
    company_id: int,
    pipeline_run_id: int,
    opp_scores: dict,
    evidence: dict,
    classified_outputs: dict | None = None,
) -> list[AIOpportunityEvidence]:
    """Convert raw evidence dicts into Evidence domain objects.

    When classified_outputs are provided, adds target_dimension and
    capture_stage from the ClassifiedEvidence items.
    """
    items = []

    # Revenue opportunity evidence (with full breakdown)
    if opp_scores.get("revenue_opportunity") is not None:
        rev_payload = {"revenue_opportunity": opp_scores["revenue_opportunity"]}
        if opp_scores.get("revenue_detail"):
            rev_payload["detail"] = opp_scores["revenue_detail"]
        items.append(AIOpportunityEvidence(
            company_id=company_id,
            pipeline_run_id=pipeline_run_id,
            evidence_type="revenue_opportunity",
            source_name="BLS / Industry Analysis",
            score_contribution=opp_scores["revenue_opportunity"],
            signal_strength="high" if opp_scores["revenue_opportunity"] > SIGNAL_STRENGTH_HIGH else "medium" if opp_scores["revenue_opportunity"] > SIGNAL_STRENGTH_MEDIUM else "low",
            target_dimension="revenue",
            payload=rev_payload,
        ))

    # Cost opportunity evidence (with full breakdown)
    if opp_scores.get("cost_opportunity") is not None:
        cost_payload = {"cost_opportunity": opp_scores["cost_opportunity"]}
        if opp_scores.get("cost_detail"):
            cost_payload["detail"] = opp_scores["cost_detail"]
        items.append(AIOpportunityEvidence(
            company_id=company_id,
            pipeline_run_id=pipeline_run_id,
            evidence_type="cost_opportunity",
            source_name="BLS / Workforce Analysis",
            score_contribution=opp_scores["cost_opportunity"],
            signal_strength="high" if opp_scores["cost_opportunity"] > SIGNAL_STRENGTH_HIGH else "medium" if opp_scores["cost_opportunity"] > SIGNAL_STRENGTH_MEDIUM else "low",
            target_dimension="cost",
            payload=cost_payload,
        ))

    # Add classified evidence from sub-scorers
    if classified_outputs:
        _add_classified_evidence(items, company_id, pipeline_run_id, classified_outputs)
    else:
        # Legacy evidence items (without classification)
        _add_legacy_evidence(items, company_id, pipeline_run_id, evidence)

    return items


def _add_classified_evidence(
    items: list[AIOpportunityEvidence],
    company_id: int,
    pipeline_run_id: int,
    classified_outputs: dict,
):
    """Add classified evidence items from sub-scorer outputs."""
    source_names = {
        "filing": "SEC EDGAR",
        "product": "GNews API / SEC EDGAR RSS",
        "job": "Adzuna API / Heuristic",
        "patent": "USPTO PatentsView API",
        "web": "Web Scrape + Gemini Flash",
        "github": "GitHub API",
        "analyst": "Yahoo Finance / Analyst Consensus",
    }
    evidence_types = {
        "filing": "filing_nlp",
        "product": "product",
        "job": "job",
        "patent": "patent",
        "web": "web_enrichment",
        "github": "github",
        "analyst": "analyst",
    }

    # Track seen keys for dedup: (evidence_type, target_dimension, keyword/patent_number)
    # When duplicates exist within a single run, keep the one with the highest raw_score
    # and track how many filings corroborate the same finding.
    seen: dict[tuple, AIOpportunityEvidence] = {}
    seen_counts: dict[tuple, int] = {}

    for key, output in classified_outputs.items():
        if output is None:
            continue
        for ev in output.evidence_items:
            ev_type = evidence_types.get(key, ev.source_type)
            target_dim = ev.target.value
            meta = ev.metadata or {}
            dedup_key = (ev_type, target_dim, meta.get("keyword", ""), meta.get("patent_number", ""))

            if dedup_key in seen:
                # Duplicate — keep highest score, increment corroboration count
                seen_counts[dedup_key] += 1
                existing = seen[dedup_key]
                if ev.raw_score > (existing.score_contribution or 0):
                    # Replace with higher-scoring item, preserve count
                    payload = dict(meta)
                    payload["filings_found_in"] = seen_counts[dedup_key]
                    new_item = AIOpportunityEvidence(
                        company_id=company_id,
                        pipeline_run_id=pipeline_run_id,
                        evidence_type=ev_type,
                        evidence_subtype=ev.source_type,
                        source_name=source_names.get(key, ""),
                        score_contribution=ev.raw_score,
                        signal_strength="high" if ev.raw_score > SIGNAL_STRENGTH_HIGH else "medium" if ev.raw_score > SIGNAL_STRENGTH_MEDIUM else "low",
                        target_dimension=target_dim,
                        capture_stage=ev.stage.value,
                        source_excerpt=ev.source_excerpt or None,
                        payload=payload,
                    )
                    # Replace in items list
                    idx = items.index(existing)
                    items[idx] = new_item
                    seen[dedup_key] = new_item
                else:
                    # Just update the corroboration count on the existing item
                    existing.payload = dict(existing.payload or {})
                    existing.payload["filings_found_in"] = seen_counts[dedup_key]
            else:
                item = AIOpportunityEvidence(
                    company_id=company_id,
                    pipeline_run_id=pipeline_run_id,
                    evidence_type=ev_type,
                    evidence_subtype=ev.source_type,
                    source_name=source_names.get(key, ""),
                    score_contribution=ev.raw_score,
                    signal_strength="high" if ev.raw_score > SIGNAL_STRENGTH_HIGH else "medium" if ev.raw_score > SIGNAL_STRENGTH_MEDIUM else "low",
                    target_dimension=target_dim,
                    capture_stage=ev.stage.value,
                    source_excerpt=ev.source_excerpt or None,
                    payload=meta,
                )
                items.append(item)
                seen[dedup_key] = item
                seen_counts[dedup_key] = 1


def _add_legacy_evidence(
    items: list[AIOpportunityEvidence],
    company_id: int,
    pipeline_run_id: int,
    evidence: dict,
):
    """Add legacy-format evidence items (backward compat)."""
    # Filing NLP evidence
    if "filing_nlp_evidence" in evidence:
        ev = evidence["filing_nlp_evidence"]
        items.append(AIOpportunityEvidence(
            company_id=company_id,
            pipeline_run_id=pipeline_run_id,
            evidence_type="filing_nlp",
            evidence_subtype="keyword_match",
            source_name="SEC EDGAR",
            payload=ev if isinstance(ev, dict) else {"raw": ev},
        ))

    # Product evidence
    if "product_evidence" in evidence:
        ev = evidence["product_evidence"]
        items.append(AIOpportunityEvidence(
            company_id=company_id,
            pipeline_run_id=pipeline_run_id,
            evidence_type="product",
            evidence_subtype="product_launch",
            source_name="GNews API / SEC EDGAR RSS",
            score_contribution=ev.get("score"),
            signal_strength="high" if ev.get("score", 0) > SIGNAL_STRENGTH_HIGH else "medium" if ev.get("score", 0) > SIGNAL_STRENGTH_MEDIUM else "low",
            payload=ev,
        ))

    # Job evidence
    if "job_evidence" in evidence:
        ev = evidence["job_evidence"]
        items.append(AIOpportunityEvidence(
            company_id=company_id,
            pipeline_run_id=pipeline_run_id,
            evidence_type="job",
            evidence_subtype=ev.get("method", "heuristic"),
            source_name=ev.get("source", {}).get("name", "Adzuna"),
            score_contribution=ev.get("score"),
            signal_strength="high" if ev.get("score", 0) > SIGNAL_STRENGTH_HIGH else "medium" if ev.get("score", 0) > SIGNAL_STRENGTH_MEDIUM else "low",
            payload=ev,
        ))

    # Patent evidence
    if "patent_evidence" in evidence:
        ev = evidence["patent_evidence"]
        items.append(AIOpportunityEvidence(
            company_id=company_id,
            pipeline_run_id=pipeline_run_id,
            evidence_type="patent",
            evidence_subtype="ai_patent",
            source_name="USPTO PatentsView API",
            score_contribution=ev.get("score"),
            signal_strength="high" if ev.get("score", 0) > SIGNAL_STRENGTH_HIGH else "medium" if ev.get("score", 0) > SIGNAL_STRENGTH_MEDIUM else "low",
            payload=ev,
        ))


def score_single_company_pipeline(
    company: CompanyModel,
) -> dict | None:
    """Score a single company using the dollar-value pipeline.

    Returns dict with dollar scores and legacy-compatible fields.
    """
    from ai_opportunity_index.data.industry_mappings import sic_to_soc_groups
    from ai_opportunity_index.scoring.pipeline.runner import ScoringPipeline

    ticker = company.ticker
    financials_obs = get_latest_financials(company.id)
    revenue_obs = financials_obs.get("revenue")
    employees_obs = financials_obs.get("employees")

    financials = {
        "revenue": revenue_obs.value if revenue_obs else 0,
        "employees": int(employees_obs.value) if employees_obs else 0,
        "sector": company.sector,
        "soc_groups": sic_to_soc_groups(company.sic) if company.sic else [],
    }

    company_dict = {
        "ticker": ticker,
        "name": company.company_name or ticker,
        "sic": company.sic,
        "sector": company.sector,
        "industry": company.industry,
    }

    pipeline = ScoringPipeline()
    dollar_score = pipeline.score_company(company_dict, financials)

    # Also compute legacy 0-1 scores for backward compat
    opp_scores = compute_opportunity_score(
        sic=company.sic,
        revenue=revenue_obs.value if revenue_obs else None,
        employees=int(employees_obs.value) if employees_obs else None,
        sector=company.sector,
        industry=company.industry,
    )

    return {
        "ticker": ticker,
        "dollar_score": dollar_score,
        "opportunity": opp_scores,
        "index": {
            "opportunity": dollar_score.opportunity,
            "realization": dollar_score.realization,
            "quadrant": dollar_score.quadrant,
            "quadrant_label": dollar_score.quadrant_label,
            "cost_opp_usd": dollar_score.cost_opportunity_usd,
            "revenue_opp_usd": dollar_score.revenue_opportunity_usd,
            "cost_capture_usd": dollar_score.cost_capture_usd,
            "revenue_capture_usd": dollar_score.revenue_capture_usd,
            "total_investment_usd": dollar_score.total_investment_usd,
        },
        "flags": [],
    }


def score_single_company(
    company: CompanyModel,
) -> dict | None:
    """Score a single company on both dimensions using the 4-value framework.

    All evidence data is read from local cache (data/raw/).
    No external API calls are made during scoring.

    Returns dict with all scores, or None on failure.
    """
    ticker = company.ticker
    name = company.company_name or ticker

    # ── Fetch latest financials from observations ──────────────────────
    financials = get_latest_financials(company.id)
    revenue_obs = financials.get("revenue")
    employees_obs = financials.get("employees")

    # ── Dollar Pipeline ────────────────────────────────────────────────
    soc_groups = sic_to_soc_groups(company.sic) if company.sic else []
    company_context = {
        "name": name,
        "ticker": ticker,
        "sector": company.sector,
        "industry": company.industry,
        "sic": company.sic,
    }
    company_financials = {
        "revenue": revenue_obs.value if revenue_obs else 0,
        "employees": int(employees_obs.value) if employees_obs else 0,
        "sector": company.sector,
        "soc_groups": soc_groups,
    }
    # Dollar pipeline disabled — ScoringPipeline has broken import
    # (get_ai_applicability). Skip to avoid wasting time on every company.
    dollar_score = None

    # ── Dimension 1: AI Opportunity ────────────────────────────────────
    opp_scores = compute_opportunity_score(
        sic=company.sic,
        revenue=revenue_obs.value if revenue_obs else None,
        employees=int(employees_obs.value) if employees_obs else None,
        sector=company.sector,
        industry=company.industry,
    )

    cost_opportunity = opp_scores.get("cost_opportunity", 0.0) or 0.0
    revenue_opportunity = opp_scores.get("revenue_opportunity", 0.0) or 0.0

    # ── Dimension 2: AI Capture (classified) ───────────────────────────
    classified_outputs: dict[str, ClassifiedScorerOutput | None] = {
        "filing": None,
        "product": None,
        "job": None,
        "patent": None,
        "web": None,
        "github": None,
        "analyst": None,
    }
    evidence = {}

    # Sub-scorer A: Filing NLP — score ALL filings and merge evidence
    filing_dir = RAW_DIR / "filings" / ticker.upper()
    if filing_dir.exists():
        filing_files = sorted(filing_dir.glob("*.txt"))
        if filing_files:
            merged_output = None
            for filing_file in filing_files:
                raw = filing_file.read_text(errors="ignore")
                text = _strip_xbrl_tags(raw) if raw.lstrip().startswith("<") else raw
                if len(text) < 200:
                    continue
                file_output = score_filing_classified(text)
                if merged_output is None:
                    merged_output = file_output
                else:
                    # Merge: take max scores, combine evidence items
                    merged_output = ClassifiedScorerOutput(
                        overall_score=max(merged_output.overall_score, file_output.overall_score),
                        cost_capture_score=max(merged_output.cost_capture_score, file_output.cost_capture_score),
                        revenue_capture_score=max(merged_output.revenue_capture_score, file_output.revenue_capture_score),
                        general_investment_score=max(merged_output.general_investment_score, file_output.general_investment_score),
                        evidence_items=merged_output.evidence_items + file_output.evidence_items,
                        raw_details=merged_output.raw_details,
                    )
            if merged_output is not None:
                classified_outputs["filing"] = merged_output

    # Sub-scorer B: Product analysis (reads from data/raw/news/)
    try:
        classified_outputs["product"] = analyze_company_products_classified(name, ticker)
    except Exception as e:
        logger.debug("Product analysis failed for %s: %s", ticker, e)

    # Sub-scorer C: Web enrichment (reads from data/raw/web_enrichment/)
    try:
        classified_outputs["web"] = score_web_enrichment_classified(name, ticker)
    except Exception as e:
        logger.debug("Web enrichment scoring failed for %s: %s", ticker, e)

    # Sub-scorer D: GitHub AI/ML activity (reads from data/raw/github/)
    try:
        classified_outputs["github"] = score_github_classified(name, ticker)
    except Exception as e:
        logger.debug("GitHub scoring failed for %s: %s", ticker, e)

    # Sub-scorer F: Analyst consensus (reads from data/raw/analysts/)
    try:
        classified_outputs["analyst"] = score_analyst_classified(name, ticker)
    except Exception as e:
        logger.debug("Analyst scoring failed for %s: %s", ticker, e)

    # Compute capture scores using 4-value framework
    capture_scores = compute_capture_scores(
        filing=classified_outputs["filing"],
        product=classified_outputs["product"],
        web=classified_outputs["web"],
        github=classified_outputs.get("github"),
        analyst=classified_outputs.get("analyst"),
    )

    # Compute 4-value index with ROI
    idx = compute_index_4v(
        cost_opportunity=cost_opportunity,
        revenue_opportunity=revenue_opportunity,
        cost_capture=capture_scores["cost_capture"],
        revenue_capture=capture_scores["revenue_capture"],
        general_investment=capture_scores["general_investment"],
    )

    # Check for discrepancies (both legacy sub-scorer and new capture-level)
    legacy_flags = flag_discrepancies({
        "filing_nlp_score": capture_scores.get("filing_nlp_score"),
        "product_score": capture_scores.get("product_score"),
    })

    capture_flags = flag_capture_discrepancies(
        cost_opportunity=cost_opportunity,
        revenue_opportunity=revenue_opportunity,
        cost_capture=capture_scores["cost_capture"],
        revenue_capture=capture_scores["revenue_capture"],
        general_investment=capture_scores["general_investment"],
    )

    flags = legacy_flags + capture_flags
    if flags:
        logger.info("Flags for %s: %s", ticker, "; ".join(flags))

    return {
        "ticker": ticker,
        "opportunity": opp_scores,
        "capture": capture_scores,
        "realization": {
            "filing_nlp_score": capture_scores.get("filing_nlp_score"),
            "product_score": capture_scores.get("product_score"),
            "composite_realization": capture_scores["composite_realization"],
        },
        "index": idx,
        "flags": flags,
        "evidence": evidence,
        "classified_outputs": classified_outputs,
        "dollar_score": dollar_score,
    }


async def score_single_company_async(
    company: CompanyModel,
    llm_semaphore: asyncio.Semaphore | None = None,
) -> dict | None:
    """Async version of score_single_company.

    Runs filing NLP and product analysis LLM calls concurrently.
    Web enrichment scoring is pure local I/O, no LLM calls.
    """
    ticker = company.ticker
    name = company.company_name or ticker

    # ── Fetch latest financials from observations ──────────────────────
    financials = get_latest_financials(company.id)
    revenue_obs = financials.get("revenue")
    employees_obs = financials.get("employees")

    # ── Dollar Pipeline ────────────────────────────────────────────────
    soc_groups = sic_to_soc_groups(company.sic) if company.sic else []
    company_context = {
        "name": name,
        "ticker": ticker,
        "sector": company.sector,
        "industry": company.industry,
        "sic": company.sic,
    }
    company_financials = {
        "revenue": revenue_obs.value if revenue_obs else 0,
        "employees": int(employees_obs.value) if employees_obs else 0,
        "sector": company.sector,
        "soc_groups": soc_groups,
    }
    # Dollar pipeline disabled — ScoringPipeline has broken import
    # (get_ai_applicability). Skip to avoid wasting time on every company.
    dollar_score = None

    # ── Dimension 1: AI Opportunity ────────────────────────────────────
    opp_scores = compute_opportunity_score(
        sic=company.sic,
        revenue=revenue_obs.value if revenue_obs else None,
        employees=int(employees_obs.value) if employees_obs else None,
        sector=company.sector,
        industry=company.industry,
    )

    cost_opportunity = opp_scores.get("cost_opportunity", 0.0) or 0.0
    revenue_opportunity = opp_scores.get("revenue_opportunity", 0.0) or 0.0

    # ── Dimension 2: AI Capture (classified, async) ────────────────────
    classified_outputs: dict[str, ClassifiedScorerOutput | None] = {
        "filing": None,
        "product": None,
        "job": None,
        "patent": None,
        "web": None,
        "github": None,
        "analyst": None,
    }
    evidence = {}

    # Try valuation pipeline first (factor-scored evidence groups)
    valuation_output = _build_scorer_from_valuations(company.id, source_label="filing_nlp")
    if valuation_output is not None:
        classified_outputs["filing"] = valuation_output
        logger.debug("[%s] Filing+news scores read from evidence valuations", ticker)
        # Skip extraction caches — valuations already incorporate all evidence
        cached_filing = valuation_output
        cached_news = ClassifiedScorerOutput(
            overall_score=0.0, cost_capture_score=0.0,
            revenue_capture_score=0.0, general_investment_score=0.0,
            evidence_items=[], raw_details={"method": "skipped_valuation_present"},
        )
        classified_outputs["product"] = cached_news
    else:
        cached_filing = None
        cached_news = None

    # Fall back to extraction cache (Phase 2: near-instant, no LLM calls)
    if cached_filing is None:
        cached_filing = _read_filing_extraction_cache(ticker)
        if cached_filing is not None:
            classified_outputs["filing"] = cached_filing
            logger.debug("[%s] Filing scores read from extraction cache", ticker)
    if cached_news is None:
        cached_news = _read_news_extraction_cache(ticker, company_name=name)
        if cached_news is not None:
            classified_outputs["product"] = cached_news
            logger.debug("[%s] News scores read from extraction cache", ticker)

    # Fall back to live LLM calls for anything not cached
    if cached_filing is None or cached_news is None:
        # Sub-scorer A: Filing NLP — score ALL filings concurrently
        async def _score_filings() -> ClassifiedScorerOutput | None:
            if cached_filing is not None:
                return cached_filing
            filing_dir = RAW_DIR / "filings" / ticker.upper()
            if not filing_dir.exists():
                return None
            filing_files = sorted(filing_dir.glob("*.txt"))
            if not filing_files:
                return None

            texts = []
            for f in filing_files:
                raw = f.read_text(errors="ignore")
                text = _strip_xbrl_tags(raw) if raw.lstrip().startswith("<") else raw
                if len(text) >= 200:
                    texts.append(text)

            if not texts:
                return None

            results = await asyncio.gather(
                *[score_filing_classified_async(t, semaphore=llm_semaphore) for t in texts],
                return_exceptions=True,
            )

            merged_output = None
            for r in results:
                if isinstance(r, Exception):
                    logger.debug("Filing extraction failed: %s", r)
                    continue
                if r.overall_score == 0.0 and not r.evidence_items:
                    continue
                if merged_output is None:
                    merged_output = r
                else:
                    merged_output = ClassifiedScorerOutput(
                        overall_score=max(merged_output.overall_score, r.overall_score),
                        cost_capture_score=max(merged_output.cost_capture_score, r.cost_capture_score),
                        revenue_capture_score=max(merged_output.revenue_capture_score, r.revenue_capture_score),
                        general_investment_score=max(merged_output.general_investment_score, r.general_investment_score),
                        evidence_items=merged_output.evidence_items + r.evidence_items,
                        raw_details=merged_output.raw_details,
                    )
            return merged_output

        # Sub-scorer B: Product analysis (async, reads from data/raw/news/)
        async def _score_products() -> ClassifiedScorerOutput | None:
            if cached_news is not None:
                return cached_news
            try:
                return await analyze_company_products_classified_async(
                    name, ticker, semaphore=llm_semaphore,
                )
            except Exception as e:
                logger.debug("Product analysis failed for %s: %s", ticker, e)
                return None

        # Run any uncached LLM calls concurrently
        filing_result, product_result = await asyncio.gather(
            _score_filings(), _score_products(),
        )
        if cached_filing is None:
            classified_outputs["filing"] = filing_result
        if cached_news is None:
            classified_outputs["product"] = product_result

    # Sub-scorer C: Web enrichment (reads from data/raw/web_enrichment/, no LLM)
    try:
        classified_outputs["web"] = score_web_enrichment_classified(name, ticker)
    except Exception as e:
        logger.debug("Web enrichment scoring failed for %s: %s", ticker, e)

    # Sub-scorer D: GitHub AI/ML activity (reads from data/raw/github/, no LLM)
    try:
        classified_outputs["github"] = score_github_classified(name, ticker)
    except Exception as e:
        logger.debug("GitHub scoring failed for %s: %s", ticker, e)

    # Sub-scorer F: Analyst consensus (reads from data/raw/analysts/, no LLM)
    try:
        classified_outputs["analyst"] = score_analyst_classified(name, ticker)
    except Exception as e:
        logger.debug("Analyst scoring failed for %s: %s", ticker, e)

    # Compute capture scores using 4-value framework
    capture_scores = compute_capture_scores(
        filing=classified_outputs["filing"],
        product=classified_outputs["product"],
        web=classified_outputs["web"],
        github=classified_outputs.get("github"),
        analyst=classified_outputs.get("analyst"),
    )

    # Compute 4-value index with ROI
    idx = compute_index_4v(
        cost_opportunity=cost_opportunity,
        revenue_opportunity=revenue_opportunity,
        cost_capture=capture_scores["cost_capture"],
        revenue_capture=capture_scores["revenue_capture"],
        general_investment=capture_scores["general_investment"],
    )

    # Check for discrepancies
    legacy_flags = flag_discrepancies({
        "filing_nlp_score": capture_scores.get("filing_nlp_score"),
        "product_score": capture_scores.get("product_score"),
    })

    capture_flags = flag_capture_discrepancies(
        cost_opportunity=cost_opportunity,
        revenue_opportunity=revenue_opportunity,
        cost_capture=capture_scores["cost_capture"],
        revenue_capture=capture_scores["revenue_capture"],
        general_investment=capture_scores["general_investment"],
    )

    flags = legacy_flags + capture_flags
    if flags:
        logger.info("Flags for %s: %s", ticker, "; ".join(flags))

    return {
        "ticker": ticker,
        "opportunity": opp_scores,
        "capture": capture_scores,
        "realization": {
            "filing_nlp_score": capture_scores.get("filing_nlp_score"),
            "product_score": capture_scores.get("product_score"),
            "composite_realization": capture_scores["composite_realization"],
        },
        "index": idx,
        "flags": flags,
        "evidence": evidence,
        "classified_outputs": classified_outputs,
        "dollar_score": dollar_score,
    }


async def score_and_save_company_async(
    company: CompanyModel,
    session,
    pipeline_run_id: int,
    now: datetime,
    dollar_pipeline: bool = False,
    llm_semaphore: asyncio.Semaphore | None = None,
) -> bool:
    """Async version of score_and_save_company.

    Uses score_single_company_async for the LLM-heavy scoring work.
    DB saves are still synchronous (fast local ops).
    """
    if dollar_pipeline:
        result = score_single_company_pipeline(company)
    else:
        result = await score_single_company_async(company, llm_semaphore=llm_semaphore)

    if not result:
        return False

    opp = result["opportunity"]
    idx = result["index"]

    # Get previous score for change detection
    prev_score = get_latest_score(company.id)

    if dollar_pipeline:
        ds = result["dollar_score"]
        score = CompanyScore(
            company_id=company.id,
            pipeline_run_id=pipeline_run_id,
            revenue_opp_score=opp.get("revenue_opportunity"),
            cost_opp_score=opp.get("cost_opportunity"),
            composite_opp_score=opp.get("composite_opportunity"),
            opportunity=idx["opportunity"],
            realization=idx["realization"],
            quadrant=idx["quadrant"],
            quadrant_label=idx["quadrant_label"],
            cost_opp_usd=ds.cost_opportunity_usd,
            revenue_opp_usd=ds.revenue_opportunity_usd,
            cost_capture_usd=ds.cost_capture_usd,
            revenue_capture_usd=ds.revenue_capture_usd,
            total_investment_usd=ds.total_investment_usd,
            flags=result.get("flags", []),
            data_as_of=now,
            scored_at=now,
        )
        save_company_score(score, session=session)

        # Save dollar-valued evidence
        dollar_evidence = _build_dollar_evidence_items(
            company.id, pipeline_run_id, ds,
        )
        if dollar_evidence:
            save_evidence_batch(dollar_evidence, session=session)
    else:
        capture = result["capture"]
        score = CompanyScore(
            company_id=company.id,
            pipeline_run_id=pipeline_run_id,
            revenue_opp_score=opp.get("revenue_opportunity"),
            cost_opp_score=opp.get("cost_opportunity"),
            composite_opp_score=opp["composite_opportunity"],
            filing_nlp_score=capture.get("filing_nlp_score"),
            product_score=capture.get("product_score"),
            github_score=capture.get("github_score"),
            analyst_score=capture.get("analyst_score"),
            composite_real_score=capture["composite_realization"],
            cost_capture_score=capture["cost_capture"],
            revenue_capture_score=capture["revenue_capture"],
            general_investment_score=capture["general_investment"],
            cost_roi=idx.get("cost_roi"),
            revenue_roi=idx.get("revenue_roi"),
            combined_roi=idx.get("combined_roi"),
            opportunity=idx["opportunity"],
            realization=idx["realization"],
            quadrant=idx["quadrant"],
            quadrant_label=idx["quadrant_label"],
            flags=result.get("flags", []),
            data_as_of=now,
            scored_at=now,
        )
        save_company_score(score, session=session)

        evidence_items = build_evidence_items(
            company_id=company.id,
            pipeline_run_id=pipeline_run_id,
            opp_scores=opp,
            evidence=result.get("evidence", {}),
            classified_outputs=result.get("classified_outputs"),
        )
        if evidence_items:
            save_evidence_batch(evidence_items, session=session)

    # Detect score changes
    if prev_score:
        if prev_score.quadrant != idx["quadrant"]:
            save_score_change(ScoreChange(
                company_id=company.id,
                dimension="composite",
                old_score=prev_score.opportunity,
                new_score=idx["opportunity"],
                old_quadrant=prev_score.quadrant,
                new_quadrant=idx["quadrant"],
            ))

    return True


def _build_dollar_evidence_items(
    company_id: int,
    pipeline_run_id: int,
    dollar_score,
) -> list[AIOpportunityEvidence]:
    """Convert ValuedEvidence from the dollar pipeline into Evidence domain objects."""
    items = []
    for ve in dollar_score.valued_evidence:
        p = ve.passage
        items.append(AIOpportunityEvidence(
            company_id=company_id,
            pipeline_run_id=pipeline_run_id,
            evidence_type=p.source_type,
            evidence_subtype=p.source_type,
            source_name=p.source_document[:100],
            score_contribution=ve.total_3yr,
            signal_strength="high" if ve.total_3yr > 1_000_000 else "medium" if ve.total_3yr > 100_000 else "low",
            target_dimension=p.target.value,
            capture_stage=p.stage.value,
            source_excerpt=p.passage_text[:500] if p.passage_text else None,
            payload={
                "dollar_year_1": ve.dollar_year_1,
                "dollar_year_2": ve.dollar_year_2,
                "dollar_year_3": ve.dollar_year_3,
                "total_3yr": ve.total_3yr,
                "horizon_shape": ve.horizon_shape,
                "valuation_method": ve.valuation_method,
                "valuation_rationale": ve.valuation_rationale,
                "confidence": p.confidence,
                **(p.metadata or {}),
            },
        ))
    return items


def score_and_save_company(
    company: CompanyModel,
    session,
    pipeline_run_id: int,
    now: datetime,
    dollar_pipeline: bool = False,
) -> bool:
    """Score a single company and save results to the database.

    Returns True if scoring succeeded, False otherwise.
    Raises on unexpected errors (caller should handle).
    """
    if dollar_pipeline:
        result = score_single_company_pipeline(company)
    else:
        result = score_single_company(company)

    if not result:
        return False

    opp = result["opportunity"]
    idx = result["index"]

    # Get previous score for change detection
    prev_score = get_latest_score(company.id)

    if dollar_pipeline:
        ds = result["dollar_score"]
        score = CompanyScore(
            company_id=company.id,
            pipeline_run_id=pipeline_run_id,
            revenue_opp_score=opp.get("revenue_opportunity"),
            cost_opp_score=opp.get("cost_opportunity"),
            composite_opp_score=opp.get("composite_opportunity"),
            opportunity=idx["opportunity"],
            realization=idx["realization"],
            quadrant=idx["quadrant"],
            quadrant_label=idx["quadrant_label"],
            cost_opp_usd=ds.cost_opportunity_usd,
            revenue_opp_usd=ds.revenue_opportunity_usd,
            cost_capture_usd=ds.cost_capture_usd,
            revenue_capture_usd=ds.revenue_capture_usd,
            total_investment_usd=ds.total_investment_usd,
            flags=result.get("flags", []),
            data_as_of=now,
            scored_at=now,
        )
        save_company_score(score, session=session)

        # Save dollar-valued evidence
        dollar_evidence = _build_dollar_evidence_items(
            company.id, pipeline_run_id, ds,
        )
        if dollar_evidence:
            save_evidence_batch(dollar_evidence, session=session)
    else:
        capture = result["capture"]
        score = CompanyScore(
            company_id=company.id,
            pipeline_run_id=pipeline_run_id,
            revenue_opp_score=opp.get("revenue_opportunity"),
            cost_opp_score=opp.get("cost_opportunity"),
            composite_opp_score=opp["composite_opportunity"],
            filing_nlp_score=capture.get("filing_nlp_score"),
            product_score=capture.get("product_score"),
            github_score=capture.get("github_score"),
            analyst_score=capture.get("analyst_score"),
            composite_real_score=capture["composite_realization"],
            cost_capture_score=capture["cost_capture"],
            revenue_capture_score=capture["revenue_capture"],
            general_investment_score=capture["general_investment"],
            cost_roi=idx.get("cost_roi"),
            revenue_roi=idx.get("revenue_roi"),
            combined_roi=idx.get("combined_roi"),
            opportunity=idx["opportunity"],
            realization=idx["realization"],
            quadrant=idx["quadrant"],
            quadrant_label=idx["quadrant_label"],
            flags=result.get("flags", []),
            data_as_of=now,
            scored_at=now,
        )
        save_company_score(score, session=session)

        evidence_items = build_evidence_items(
            company_id=company.id,
            pipeline_run_id=pipeline_run_id,
            opp_scores=opp,
            evidence=result.get("evidence", {}),
            classified_outputs=result.get("classified_outputs"),
        )
        if evidence_items:
            save_evidence_batch(evidence_items, session=session)

    # Detect score changes
    if prev_score:
        if prev_score.quadrant != idx["quadrant"]:
            save_score_change(ScoreChange(
                company_id=company.id,
                dimension="composite",
                old_score=prev_score.opportunity,
                new_score=idx["opportunity"],
                old_quadrant=prev_score.quadrant,
                new_quadrant=idx["quadrant"],
            ))

    return True


def main():
    parser = argparse.ArgumentParser(description="Score companies on AI opportunity and realization")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of companies to score")
    parser.add_argument("--tickers", nargs="*", help="Specific tickers to score")
    parser.add_argument("--dollar-pipeline", action="store_true", help="Use dollar-value pipeline instead of 0-1 scoring")
    args = parser.parse_args()

    init_db()
    session = get_session()
    now = datetime.utcnow()
    run_type = "partial" if args.tickers else "full"
    ticker_list = [t.upper() for t in args.tickers] if args.tickers else []

    # Get companies to score
    query = session.query(CompanyModel)
    if args.tickers:
        query = query.filter(CompanyModel.ticker.in_(ticker_list))
    if args.limit:
        query = query.limit(args.limit)

    companies = query.all()
    if not ticker_list:
        ticker_list = [c.ticker for c in companies]

    # Create extract stage run
    extract_run_id = str(uuid.uuid4())
    extract_run = create_pipeline_run(PipelineRun(
        run_id=extract_run_id,
        task=PipelineTask.EXTRACT,
        subtask=PipelineSubtask.ALL,
        run_type=run_type,
        status="running",
        tickers_requested=ticker_list,
        parameters={"capture_weights": CAPTURE_WEIGHTS},
    ))

    # Create value stage run (chained to extract)
    value_run_id = str(uuid.uuid4())
    value_run = create_pipeline_run(PipelineRun(
        run_id=value_run_id,
        task=PipelineTask.VALUE,
        subtask=PipelineSubtask.ALL,
        run_type=run_type,
        status="running",
        tickers_requested=ticker_list,
        parent_run_id=extract_run_id,
        parameters={"opportunity_weights": OPPORTUNITY_WEIGHTS},
    ))

    # Create score stage run (chained to value)
    score_run_id = str(uuid.uuid4())
    score_run = create_pipeline_run(PipelineRun(
        run_id=score_run_id,
        task=PipelineTask.SCORE,
        subtask=PipelineSubtask.ALL,
        run_type=run_type,
        status="running",
        tickers_requested=ticker_list,
        parent_run_id=value_run_id,
        parameters={
            "opportunity_weights": OPPORTUNITY_WEIGHTS,
            "capture_weights": CAPTURE_WEIGHTS,
        },
    ))

    try:
        logger.info("=== Scoring %d companies (run: %s) ===", len(companies), score_run_id[:8])

        scored = 0
        failed = 0
        for i, company in enumerate(companies):
            try:
                success = score_and_save_company(
                    company, session, score_run.id, now,
                    dollar_pipeline=args.dollar_pipeline,
                )
                if success:
                    scored += 1

                if (i + 1) % 100 == 0:
                    session.commit()
                    logger.info("Scoring progress: %d/%d (scored: %d)", i + 1, len(companies), scored)

            except Exception as e:
                logger.warning("Failed to score %s: %s", company.ticker, e)
                failed += 1

        session.commit()

        # Complete all pipeline stage runs
        complete_pipeline_run(
            run_id=extract_run_id,
            status="completed",
            tickers_succeeded=scored,
            tickers_failed=failed,
        )
        complete_pipeline_run(
            run_id=value_run_id,
            status="completed",
            tickers_succeeded=scored,
            tickers_failed=failed,
        )
        complete_pipeline_run(
            run_id=score_run_id,
            status="completed",
            tickers_succeeded=scored,
            tickers_failed=failed,
        )

        # Refresh materialized view — retry once on failure since CONCURRENTLY
        # can fail due to lock contention.  Dashboard reads from this view so
        # a stale view causes score mismatches vs the detail page.
        for attempt in range(2):
            try:
                refresh_latest_scores_view()
                break
            except Exception as e:
                if attempt == 0:
                    logger.warning("Materialized view refresh failed (attempt 1), retrying: %s", e)
                else:
                    logger.error(
                        "Materialized view refresh FAILED after 2 attempts: %s. "
                        "Dashboard will show stale scores until manually refreshed.",
                        e,
                    )

        logger.info("=== Scoring complete: %d/%d companies scored, %d failed ===", scored, len(companies), failed)

    except Exception as exc:
        error_msg = str(exc)
        complete_pipeline_run(run_id=extract_run_id, status="failed", error_message=error_msg)
        complete_pipeline_run(run_id=value_run_id, status="failed", error_message=error_msg)
        complete_pipeline_run(run_id=score_run_id, status="failed", error_message=error_msg)
        raise
    finally:
        session.close()


if __name__ == "__main__":
    main()
