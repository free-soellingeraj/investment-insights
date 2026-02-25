"""Pipeline orchestrator: runs the 4-stage evidence-to-dollar pipeline.

Stage 1: Collect (external — collect_evidence.py)
Stage 2: Extract — LLM extractors for filing/news, keyword for patent/job
Stage 3: Value — formula or LLM estimators produce ValuedEvidence objects
Stage 4: Score — aggregate into CompanyDollarScore
"""

from __future__ import annotations

import logging

from ai_opportunity_index.config import (
    DOLLAR_PRODUCTIVITY_GAIN_PCT,
    DOLLAR_REVENUE_PENETRATION_RATE,
    QUADRANT_LABELS,
    QUADRANT_OPP_THRESHOLD,
    QUADRANT_REAL_THRESHOLD,
    RAW_DIR,
)
from ai_opportunity_index.data.industry_mappings import sic_to_soc_groups
from ai_opportunity_index.scoring.evidence_classification import (
    CaptureStage,
    TargetDimension,
)
from ai_opportunity_index.scoring.pipeline.estimators import FormulaDollarEstimator
from ai_opportunity_index.scoring.pipeline.models import (
    CompanyDollarScore,
    EvidencePassage,
    ValuedEvidence,
)

logger = logging.getLogger(__name__)


class ScoringPipeline:
    """Orchestrates the 4-stage evidence-to-dollar pipeline."""

    def __init__(self, use_llm: bool = False):
        # Filing and news always use LLM extractors
        try:
            from ai_opportunity_index.scoring.pipeline.llm_extractors import (
                LLMFilingExtractor,
                LLMNewsExtractor,
            )
            self.extractors = {
                "filing": LLMFilingExtractor(),
                "news": LLMNewsExtractor(),
            }
        except ImportError:
            raise NotImplementedError(
                "LLM pipeline requires pydantic-ai. Install with: pip install pydantic-ai"
            )

        # Estimator can be formula or LLM based
        if use_llm:
            try:
                from ai_opportunity_index.scoring.pipeline.llm_estimators import (
                    LLMDollarEstimator,
                )
                self.estimator = LLMDollarEstimator()
            except ImportError:
                raise NotImplementedError(
                    "LLM estimator requires pydantic-ai. Install with: pip install pydantic-ai"
                )
        else:
            self.estimator = FormulaDollarEstimator()

    def score_company(
        self,
        company: dict,
        financials: dict,
        raw_documents: dict[str, list[str]] | None = None,
    ) -> CompanyDollarScore:
        """Run the full pipeline for a company.

        Args:
            company: Dict with keys: ticker, name, sic, sector, industry, etc.
            financials: Dict with keys: revenue, employees, sector, soc_groups, etc.
            raw_documents: Optional dict of source_type -> list of document texts.
                           If not provided, loads from data/raw/ cache.

        Returns:
            CompanyDollarScore with dollar totals and valued evidence.
        """
        ticker = company.get("ticker", "")
        company_context = {
            "name": company.get("name", company.get("company_name", "")),
            "ticker": ticker,
            "sector": company.get("sector", ""),
            "industry": company.get("industry", ""),
            "revenue": financials.get("revenue", 0),
            "employees": financials.get("employees", 0),
        }

        # Ensure SOC groups are in financials
        if "soc_groups" not in financials and company.get("sic"):
            financials["soc_groups"] = sic_to_soc_groups(company["sic"])

        # ── Stage 2: Extract ─────────────────────────────────────────────
        all_passages: list[EvidencePassage] = []

        if raw_documents:
            for source_type, docs in raw_documents.items():
                extractor = self.extractors.get(source_type)
                if not extractor:
                    continue
                for doc in docs:
                    passages = extractor.extract(doc, source_type, company_context)
                    all_passages.extend(passages)
        else:
            # Load from cache
            all_passages.extend(self._extract_from_cache(ticker, company_context))

        logger.debug(
            "Extracted %d passages for %s", len(all_passages), ticker
        )

        # ── Stage 3: Value ───────────────────────────────────────────────
        valued: list[ValuedEvidence] = []
        for passage in all_passages:
            try:
                ve = self.estimator.estimate(passage, financials)
                valued.append(ve)
            except Exception as e:
                logger.debug("Estimation failed for passage: %s", e)

        # ── Stage 4: Score (aggregate) ───────────────────────────────────
        return self._aggregate(valued, company, financials)

    def _extract_from_cache(
        self, ticker: str, company_context: dict
    ) -> list[EvidencePassage]:
        """Extract evidence from cached raw data files."""
        passages: list[EvidencePassage] = []

        # Filings
        filing_dir = RAW_DIR / "filings" / ticker.upper()
        if filing_dir.exists():
            for filing_file in sorted(filing_dir.glob("*.txt")):
                text = filing_file.read_text(errors="ignore")
                if len(text) < 100:
                    continue
                ctx = {**company_context, "filing_name": filing_file.name}
                extractor = self.extractors["filing"]
                passages.extend(extractor.extract(text, "filing", ctx))

        # News — extractor handles cache loading internally
        news_extractor = self.extractors["news"]
        passages.extend(news_extractor.extract("", "news", company_context))

        return passages

    def _aggregate(
        self,
        valued_evidence: list[ValuedEvidence],
        company: dict,
        financials: dict,
    ) -> CompanyDollarScore:
        """Aggregate valued evidence into company-level dollar scores."""
        cost_3yr = 0.0
        revenue_3yr = 0.0
        general_3yr = 0.0

        cost_y1 = cost_y2 = cost_y3 = 0.0
        rev_y1 = rev_y2 = rev_y3 = 0.0

        cost_count = 0
        revenue_count = 0
        general_count = 0

        for v in valued_evidence:
            if v.passage.target == TargetDimension.COST:
                cost_3yr += v.total_3yr
                cost_y1 += v.dollar_year_1
                cost_y2 += v.dollar_year_2
                cost_y3 += v.dollar_year_3
                cost_count += 1
            elif v.passage.target == TargetDimension.REVENUE:
                revenue_3yr += v.total_3yr
                rev_y1 += v.dollar_year_1
                rev_y2 += v.dollar_year_2
                rev_y3 += v.dollar_year_3
                revenue_count += 1
            else:
                general_3yr += v.total_3yr
                general_count += 1

        total_investment = cost_3yr + revenue_3yr + general_3yr

        # Compute cost/revenue opportunity in dollar terms
        cost_opp_usd = self._compute_cost_opportunity_usd(financials)
        rev_opp_usd = self._compute_revenue_opportunity_usd(financials)

        # Quadrant assignment based on dollar values
        # Use relative thresholds: if capture > 50% of opportunity, it's "high"
        cost_ratio = cost_3yr / max(cost_opp_usd, 1.0)
        rev_ratio = revenue_3yr / max(rev_opp_usd, 1.0)

        # Normalize to 0-1 for quadrant (cap at 1.0)
        opp_norm = min(1.0, (cost_opp_usd + rev_opp_usd) / max(cost_opp_usd + rev_opp_usd, 1.0))
        real_norm = min(1.0, (cost_ratio + rev_ratio) / 2.0)

        quadrant, quadrant_label = self._assign_quadrant(opp_norm, real_norm)

        return CompanyDollarScore(
            cost_opportunity_usd=round(cost_opp_usd, 2),
            revenue_opportunity_usd=round(rev_opp_usd, 2),
            cost_capture_usd=round(cost_3yr, 2),
            revenue_capture_usd=round(revenue_3yr, 2),
            total_investment_usd=round(total_investment, 2),
            cost_capture_y1=round(cost_y1, 2),
            cost_capture_y2=round(cost_y2, 2),
            cost_capture_y3=round(cost_y3, 2),
            revenue_capture_y1=round(rev_y1, 2),
            revenue_capture_y2=round(rev_y2, 2),
            revenue_capture_y3=round(rev_y3, 2),
            general_investment_usd=round(general_3yr, 2),
            evidence_count=len(valued_evidence),
            cost_evidence_count=cost_count,
            revenue_evidence_count=revenue_count,
            general_evidence_count=general_count,
            quadrant=quadrant,
            quadrant_label=quadrant_label,
            valued_evidence=valued_evidence,
            opportunity=round(opp_norm, 4),
            realization=round(real_norm, 4),
        )

    def _compute_cost_opportunity_usd(self, financials: dict) -> float:
        """Total addressable cost savings from AI in dollar terms."""
        from ai_opportunity_index.scoring.pipeline.estimators import (
            get_ai_applicability,
            get_sector_avg_salary,
        )

        employees = financials.get("employees", 0) or 0
        soc_groups = financials.get("soc_groups", [])
        sector = financials.get("sector")

        avg_salary = get_sector_avg_salary(sector, soc_groups)
        ai_applicability = get_ai_applicability(soc_groups)

        return employees * avg_salary * ai_applicability * DOLLAR_PRODUCTIVITY_GAIN_PCT

    def _compute_revenue_opportunity_usd(self, financials: dict) -> float:
        """Total addressable revenue from AI in dollar terms."""
        revenue = financials.get("revenue", 0) or 0
        return revenue * DOLLAR_REVENUE_PENETRATION_RATE

    def _assign_quadrant(
        self, opportunity: float, realization: float
    ) -> tuple[str, str]:
        """Assign quadrant based on normalized scores."""
        if opportunity >= QUADRANT_OPP_THRESHOLD:
            if realization >= QUADRANT_REAL_THRESHOLD:
                q = "high_opp_high_real"
            else:
                q = "high_opp_low_real"
        else:
            if realization >= QUADRANT_REAL_THRESHOLD:
                q = "low_opp_high_real"
            else:
                q = "low_opp_low_real"

        return q, QUADRANT_LABELS[q]
