"""Sub-scorer A: NLP analysis of SEC filings for AI signals.

Uses Gemini Flash via pydantic_ai with structured output to score how
prominently AI features in a company's financial filings. Returns
ClassifiedScorerOutput with cost/revenue/general classification.
"""

import asyncio
import json
import logging

from ai_opportunity_index.config import LLM_EXTRACTION_MODEL, get_google_provider
from ai_opportunity_index.prompts.loader import load_prompt
from ai_opportunity_index.scoring.evidence_classification import (
    CaptureStage,
    ClassifiedEvidence,
    ClassifiedScorerOutput,
    TargetDimension,
)

logger = logging.getLogger(__name__)

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


def _get_agent():
    """Lazy-init pydantic_ai agent for filing extraction."""
    from pydantic_ai import Agent
    from pydantic_ai.models.google import GoogleModel

    from ai_opportunity_index.scoring.pipeline.llm_extractors import ExtractedPassages

    model = GoogleModel(LLM_EXTRACTION_MODEL, provider=get_google_provider())
    return Agent(model, output_type=ExtractedPassages)


def score_filing_classified(text: str) -> ClassifiedScorerOutput:
    """Compute classified filing NLP scores with cost/revenue/general breakdown.

    Uses Gemini Flash to extract structured evidence from filing text.
    Returns ClassifiedScorerOutput with target-dimension scores and evidence.
    """
    if not text or len(text) < 100:
        return ClassifiedScorerOutput(overall_score=0.0)

    try:
        agent = _get_agent()
        prompt = load_prompt(
            "extract_filing_evidence",
            company_name="",
            ticker="",
            sector="",
            revenue=0,
            employees=0,
            document_text=text[:8000],
        )
        result = agent.run_sync(prompt)
        extracted = result.output
    except Exception as e:
        logger.warning("LLM filing extraction failed: %s", e)
        return ClassifiedScorerOutput(overall_score=0.0)

    # Convert extracted passages to ClassifiedEvidence and accumulate scores
    evidence_items: list[ClassifiedEvidence] = []
    cost_score = 0.0
    revenue_score = 0.0
    general_score = 0.0

    for p in extracted.passages:
        target = _TARGET_MAP.get(p.target_dimension, TargetDimension.GENERAL)
        stage = _STAGE_MAP.get(p.capture_stage, CaptureStage.INVESTED)
        confidence = max(0.0, min(1.0, p.confidence))

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
            description=p.reasoning[:200] if p.reasoning else "LLM-extracted filing evidence",
            source_excerpt=p.passage_text[:500],
            metadata={"method": "llm", "reasoning": p.reasoning},
        ))

    # Normalize scores to 0-1 range (cap at 1.0)
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
        raw_details={"method": "gemini_flash", "passages_extracted": len(evidence_items)},
    )


async def score_filing_classified_async(
    text: str,
    semaphore: asyncio.Semaphore | None = None,
) -> ClassifiedScorerOutput:
    """Async version of score_filing_classified.

    Uses ``await agent.run(prompt)`` instead of ``agent.run_sync(prompt)``.
    An optional semaphore gates concurrent LLM calls.
    """
    if not text or len(text) < 100:
        return ClassifiedScorerOutput(overall_score=0.0)

    try:
        agent = _get_agent()
        prompt = load_prompt(
            "extract_filing_evidence",
            company_name="",
            ticker="",
            sector="",
            revenue=0,
            employees=0,
            document_text=text[:8000],
        )
        if semaphore:
            async with semaphore:
                result = await agent.run(prompt)
        else:
            result = await agent.run(prompt)
        usage = result.usage()
        logger.info(
            "Filing NLP LLM (fallback): input=%d output=%d total=%d tokens",
            usage.input_tokens or 0, usage.output_tokens or 0, usage.total_tokens or 0,
        )
        extracted = result.output
    except Exception as e:
        logger.warning("LLM filing extraction failed: %s", e)
        return ClassifiedScorerOutput(overall_score=0.0)

    # Convert extracted passages to ClassifiedEvidence and accumulate scores
    evidence_items: list[ClassifiedEvidence] = []
    cost_score = 0.0
    revenue_score = 0.0
    general_score = 0.0

    for p in extracted.passages:
        target = _TARGET_MAP.get(p.target_dimension, TargetDimension.GENERAL)
        stage = _STAGE_MAP.get(p.capture_stage, CaptureStage.INVESTED)
        confidence = max(0.0, min(1.0, p.confidence))

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
            description=p.reasoning[:200] if p.reasoning else "LLM-extracted filing evidence",
            source_excerpt=p.passage_text[:500],
            metadata={"method": "llm", "reasoning": p.reasoning},
        ))

    # Normalize scores to 0-1 range (cap at 1.0)
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
        raw_details={"method": "gemini_flash", "passages_extracted": len(evidence_items)},
    )
