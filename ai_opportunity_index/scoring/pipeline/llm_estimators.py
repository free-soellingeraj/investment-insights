"""LLM-based dollar estimators using Pydantic AI with structured output.

Uses Gemini Flash (default) or Claude Haiku (fallback) for estimation.
Guarded behind --use-llm flag; not active in default prototype.
"""

from __future__ import annotations

import logging
from typing import Literal

from pydantic import BaseModel

from ai_opportunity_index.config import LLM_ESTIMATION_MODEL
from ai_opportunity_index.prompts import load_prompt
from ai_opportunity_index.scoring.pipeline.base import DollarEstimator
from ai_opportunity_index.scoring.pipeline.models import (
    EvidencePassage,
    ValuedEvidence,
)

logger = logging.getLogger(__name__)


class DollarEstimate(BaseModel):
    """Structured output for LLM dollar estimation."""

    annual_dollar_impact: float
    year_1_pct: float  # % of full impact realized in year 1
    year_2_pct: float
    year_3_pct: float
    horizon_shape: Literal["flat", "linear_ramp", "s_curve", "back_loaded"]
    rationale: str


class LLMDollarEstimator(DollarEstimator):
    """Estimate dollar impact using LLM analysis."""

    def __init__(self) -> None:
        from pydantic_ai import Agent
        from pydantic_ai.models.google import GoogleModel

        from ai_opportunity_index.config import get_google_provider

        model = GoogleModel(LLM_ESTIMATION_MODEL, provider=get_google_provider())
        self.agent = Agent(
            model,
            output_type=DollarEstimate,
            system_prompt=(
                "You are a financial analyst who estimates the annual dollar impact "
                "of AI initiatives for public companies. Be conservative and realistic. "
                "Base your estimates on company size, industry benchmarks, and the "
                "specificity of the evidence provided."
            ),
        )

    async def _estimate_async(
        self,
        passage: EvidencePassage,
        company_financials: dict,
    ) -> ValuedEvidence:
        prompt = load_prompt(
            "estimate_dollar_impact",
            company_name=company_financials.get("name", ""),
            revenue=company_financials.get("revenue", 0),
            employees=company_financials.get("employees", 0),
            sector=company_financials.get("sector", ""),
            target_dimension=passage.target.value,
            capture_stage=passage.stage.value,
            passage_text=passage.passage_text,
        )
        result = await self.agent.run(prompt)
        est = result.output
        base = est.annual_dollar_impact
        y1 = base * est.year_1_pct
        y2 = base * est.year_2_pct
        y3 = base * est.year_3_pct
        return ValuedEvidence(
            passage=passage,
            dollar_year_1=round(y1, 2),
            dollar_year_2=round(y2, 2),
            dollar_year_3=round(y3, 2),
            total_3yr=round(y1 + y2 + y3, 2),
            horizon_shape=est.horizon_shape,
            valuation_method="llm",
            valuation_rationale=est.rationale,
        )

    def estimate(
        self,
        passage: EvidencePassage,
        company_financials: dict,
    ) -> ValuedEvidence:
        import asyncio

        return asyncio.run(
            self._estimate_async(passage, company_financials)
        )
