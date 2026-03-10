"""LLM-based evidence extractors using Pydantic AI with structured output.

Uses Gemini Flash (default, cheapest) or Claude Haiku (fallback).
Guarded behind --use-llm flag; the formula implementations run by default.
"""

from __future__ import annotations

import logging

from typing import Annotated

from pydantic import BaseModel, BeforeValidator

from ai_opportunity_index.config import LLM_EXTRACTION_MODEL
from ai_opportunity_index.domains import CaptureStage, TargetDimension


def _lower_strip(v: str) -> str:
    """Normalize LLM enum output to lowercase."""
    if isinstance(v, str):
        return v.strip().lower()
    return v
from ai_opportunity_index.prompts.loader import load_prompt
from ai_opportunity_index.scoring.pipeline.base import EvidenceExtractor
from ai_opportunity_index.scoring.pipeline.models import EvidencePassage

logger = logging.getLogger(__name__)


# ── Structured output models ─────────────────────────────────────────────


class ExtractedPassage(BaseModel):
    """Structured output for a single extracted evidence passage."""

    passage_text: str
    target_dimension: Annotated[TargetDimension, BeforeValidator(_lower_strip)]
    capture_stage: Annotated[CaptureStage, BeforeValidator(_lower_strip)]
    confidence: float  # 0-1
    reasoning: str


class ExtractedPassages(BaseModel):
    """Batch extraction result."""

    passages: list[ExtractedPassage]


def _to_evidence_passages(
    extracted: ExtractedPassages,
    source_type: str,
    source_document: str,
) -> list[EvidencePassage]:
    """Convert LLM output to EvidencePassage objects.

    The ExtractedPassage model uses TargetDimension/CaptureStage enums directly.
    Pydantic handles case-insensitive coercion for str enums, but we add fallback
    defaults in case the LLM returns an unexpected value.
    """
    results = []
    for p in extracted.passages:
        # Pydantic already coerces to enum; use directly with safe fallback
        target = p.target_dimension if isinstance(p.target_dimension, TargetDimension) else TargetDimension.GENERAL
        stage = p.capture_stage if isinstance(p.capture_stage, CaptureStage) else CaptureStage.INVESTED
        results.append(EvidencePassage(
            source_type=source_type,
            source_document=source_document,
            passage_text=p.passage_text[:500],
            target=target,
            stage=stage,
            confidence=max(0.0, min(1.0, p.confidence)),
            metadata={"reasoning": p.reasoning, "method": "llm"},
        ))
    return results


# ── LLM Extractors ──────────────────────────────────────────────────────


class LLMFilingExtractor(EvidenceExtractor):
    """Extract AI evidence from SEC filings using LLM analysis."""

    def __init__(self):
        try:
            from pydantic_ai import Agent
            from pydantic_ai.models.google import GoogleModel

            from ai_opportunity_index.config import get_google_provider

            model = GoogleModel(LLM_EXTRACTION_MODEL, provider=get_google_provider())
            self.agent = Agent(
                model,
                output_type=ExtractedPassages,
            )
        except ImportError:
            raise ImportError(
                "pydantic-ai is required for LLM extractors. "
                "Install with: pip install pydantic-ai"
            )

    def extract(
        self,
        document_text: str,
        source_type: str,
        company_context: dict,
    ) -> list[EvidencePassage]:
        if not document_text or len(document_text) < 100:
            return []

        prompt = load_prompt(
            "extract_filing_evidence",
            company_name=company_context.get("name", ""),
            ticker=company_context.get("ticker", ""),
            sector=company_context.get("sector", ""),
            revenue=company_context.get("revenue", 0),
            employees=company_context.get("employees", 0),
            document_text=document_text[:8000],
        )

        try:
            result = self.agent.run_sync(prompt)
            return _to_evidence_passages(
                result.output,
                source_type="filing",
                source_document=company_context.get("filing_name", "SEC filing"),
            )
        except Exception as e:
            logger.warning("LLM filing extraction failed: %s", e)
            return []


class LLMNewsExtractor(EvidenceExtractor):
    """Extract AI evidence from news articles using LLM analysis."""

    def __init__(self):
        try:
            from pydantic_ai import Agent
            from pydantic_ai.models.google import GoogleModel

            from ai_opportunity_index.config import get_google_provider

            model = GoogleModel(LLM_EXTRACTION_MODEL, provider=get_google_provider())
            self.agent = Agent(
                model,
                output_type=ExtractedPassages,
            )
        except ImportError:
            raise ImportError(
                "pydantic-ai is required for LLM extractors. "
                "Install with: pip install pydantic-ai"
            )

    def extract(
        self,
        document_text: str,
        source_type: str,
        company_context: dict,
    ) -> list[EvidencePassage]:
        if not document_text:
            return []

        prompt = load_prompt(
            "extract_news_evidence",
            company_name=company_context.get("name", ""),
            ticker=company_context.get("ticker", ""),
            sector=company_context.get("sector", ""),
            revenue=company_context.get("revenue", 0),
            document_text=document_text[:4000],
        )

        try:
            result = self.agent.run_sync(prompt)
            return _to_evidence_passages(
                result.output,
                source_type="news",
                source_document=document_text[:200],
            )
        except Exception as e:
            logger.warning("LLM news extraction failed: %s", e)
            return []
