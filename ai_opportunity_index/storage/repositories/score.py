"""Async repository for score operations."""

from __future__ import annotations

import logging

import pandas as pd
from sqlalchemy import func, select, text

from ai_opportunity_index.domains import CompanyScore, ScoreChange
from ai_opportunity_index.storage.models import (
    CompanyModel,
    CompanyScoreModel,
    ScoreChangeModel,
)
from ai_opportunity_index.storage.repositories.base import BaseRepository

logger = logging.getLogger(__name__)


class ScoreRepository(BaseRepository[CompanyScoreModel]):
    """Async repository for company score CRUD and dashboard queries."""

    model_class = CompanyScoreModel

    async def count(self) -> int:
        """Count distinct companies that have been scored (not total score rows)."""
        stmt = select(func.count(func.distinct(CompanyScoreModel.company_id)))
        result = await self.session.execute(stmt)
        return result.scalar() or 0

    @staticmethod
    def _to_domain(m: CompanyScoreModel) -> CompanyScore:
        return CompanyScore(
            id=m.id,
            company_id=m.company_id,
            pipeline_run_id=m.pipeline_run_id,
            revenue_opp_score=m.revenue_opp_score,
            cost_opp_score=m.cost_opp_score,
            composite_opp_score=m.composite_opp_score,
            filing_nlp_score=m.filing_nlp_score,
            product_score=m.product_score,
            github_score=m.github_score,
            analyst_score=m.analyst_score,
            composite_real_score=m.composite_real_score,
            cost_capture_score=m.cost_capture_score,
            revenue_capture_score=m.revenue_capture_score,
            general_investment_score=m.general_investment_score,
            cost_roi=m.cost_roi,
            revenue_roi=m.revenue_roi,
            combined_roi=m.combined_roi,
            cost_opp_usd=m.cost_opp_usd,
            revenue_opp_usd=m.revenue_opp_usd,
            cost_capture_usd=m.cost_capture_usd,
            revenue_capture_usd=m.revenue_capture_usd,
            total_investment_usd=m.total_investment_usd,
            ai_index_usd=m.ai_index_usd,
            capture_probability=m.capture_probability,
            opportunity_usd=m.opportunity_usd,
            evidence_dollars=m.evidence_dollars,
            opportunity=m.opportunity,
            realization=m.realization,
            quadrant=m.quadrant,
            quadrant_label=m.quadrant_label,
            combined_rank=m.combined_rank,
            flags=m.flags or [],
            data_as_of=m.data_as_of,
            scored_at=m.scored_at,
        )

    async def save_company_score(self, score: CompanyScore) -> CompanyScore:
        """Save a unified company score."""
        model = CompanyScoreModel(
            company_id=score.company_id,
            pipeline_run_id=score.pipeline_run_id,
            revenue_opp_score=score.revenue_opp_score,
            cost_opp_score=score.cost_opp_score,
            composite_opp_score=score.composite_opp_score,
            filing_nlp_score=score.filing_nlp_score,
            product_score=score.product_score,
            github_score=score.github_score,
            analyst_score=score.analyst_score,
            composite_real_score=score.composite_real_score,
            cost_capture_score=score.cost_capture_score,
            revenue_capture_score=score.revenue_capture_score,
            general_investment_score=score.general_investment_score,
            cost_roi=score.cost_roi,
            revenue_roi=score.revenue_roi,
            combined_roi=score.combined_roi,
            opportunity=score.opportunity,
            realization=score.realization,
            quadrant=score.quadrant,
            quadrant_label=score.quadrant_label,
            combined_rank=score.combined_rank,
            cost_opp_usd=score.cost_opp_usd,
            revenue_opp_usd=score.revenue_opp_usd,
            cost_capture_usd=score.cost_capture_usd,
            revenue_capture_usd=score.revenue_capture_usd,
            total_investment_usd=score.total_investment_usd,
            ai_index_usd=score.ai_index_usd,
            capture_probability=score.capture_probability,
            opportunity_usd=score.opportunity_usd,
            evidence_dollars=score.evidence_dollars,
            flags=score.flags,
            data_as_of=score.data_as_of,
            scored_at=score.scored_at,
        )
        self.session.add(model)
        await self.session.flush()
        return self._to_domain(model)

    async def get_latest_score(self, company_id: int) -> CompanyScore | None:
        """Get the most recent score for a company."""
        result = await self.session.execute(
            select(CompanyScoreModel)
            .where(CompanyScoreModel.company_id == company_id)
            .order_by(CompanyScoreModel.scored_at.desc())
            .limit(1)
        )
        model = result.scalar_one_or_none()
        return self._to_domain(model) if model else None

    async def get_latest_scores(
        self,
        sector: str | None = None,
        quadrant: str | None = None,
        industry: str | None = None,
        sort_by: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """Dashboard query -- uses materialized view for fast reads."""
        params: dict = {}
        filters: list[str] = []

        if sector:
            filters.append("sector = :sector")
            params["sector"] = sector
        if quadrant:
            filters.append("quadrant = :quadrant")
            params["quadrant"] = quadrant
        if industry:
            filters.append("industry = :industry")
            params["industry"] = industry

        # Validate sort_by to prevent SQL injection
        allowed_sort = {
            "opportunity", "realization", "cost_capture_score", "revenue_capture_score",
            "general_investment_score", "cost_roi", "revenue_roi", "combined_roi",
            "revenue_opp_score", "cost_opp_score", "ticker", "company_name",
            "ai_index_usd", "opportunity_usd",
        }
        order_col = sort_by if sort_by in allowed_sort else "opportunity"
        order_dir = "DESC" if order_col not in ("ticker", "company_name") else "ASC"
        # Add company_id tiebreaker to prevent pagination overlap when primary sort has ties
        order_clause = f"{order_col} {order_dir} NULLS LAST, company_id ASC"

        where_clause = " AND ".join(filters)
        if where_clause:
            sql = (
                f"SELECT * FROM latest_company_scores WHERE {where_clause} "
                f"ORDER BY {order_clause} "
                f"LIMIT :limit OFFSET :offset"
            )
        else:
            sql = (
                f"SELECT * FROM latest_company_scores "
                f"ORDER BY {order_clause} "
                f"LIMIT :limit OFFSET :offset"
            )

        params["limit"] = limit
        params["offset"] = offset

        result = await self.session.execute(text(sql), params)
        columns = result.keys()
        return [dict(zip(columns, row)) for row in result.fetchall()]

    async def get_full_index(self) -> pd.DataFrame:
        """Retrieve the full index as a DataFrame (backwards compat for portfolio computation).

        Uses the materialized view, falling back to a manual join if unavailable.
        """
        try:
            result = await self.session.execute(text("SELECT * FROM latest_company_scores"))
            columns = result.keys()
            rows = result.fetchall()
            return pd.DataFrame(rows, columns=columns)
        except Exception:
            # Fall back to manual join if materialized view doesn't exist
            latest = (
                select(
                    CompanyScoreModel.company_id,
                    func.max(CompanyScoreModel.scored_at).label("max_scored"),
                )
                .group_by(CompanyScoreModel.company_id)
                .subquery()
            )
            stmt = (
                select(
                    CompanyModel.ticker,
                    CompanyModel.company_name,
                    CompanyModel.sic,
                    CompanyModel.exchange,
                    CompanyModel.sector,
                    CompanyModel.industry,
                    CompanyScoreModel.opportunity,
                    CompanyScoreModel.realization,
                    CompanyScoreModel.quadrant,
                    CompanyScoreModel.quadrant_label,
                    CompanyScoreModel.scored_at,
                )
                .join(CompanyScoreModel, CompanyModel.id == CompanyScoreModel.company_id)
                .join(
                    latest,
                    (CompanyScoreModel.company_id == latest.c.company_id)
                    & (CompanyScoreModel.scored_at == latest.c.max_scored),
                )
            )
            result = await self.session.execute(stmt)
            columns = result.keys()
            rows = result.fetchall()
            return pd.DataFrame(rows, columns=columns)

    async def refresh_latest_scores_view(self) -> None:
        """Refresh the materialized view after a scoring run."""
        await self.session.execute(
            text("REFRESH MATERIALIZED VIEW CONCURRENTLY latest_company_scores")
        )
        await self.session.flush()
        logger.info("Refreshed latest_company_scores materialized view")

    async def save_score_change(self, change: ScoreChange) -> None:
        """Record a score change."""
        model = ScoreChangeModel(
            company_id=change.company_id,
            dimension=change.dimension,
            old_score=change.old_score,
            new_score=change.new_score,
            old_quadrant=change.old_quadrant,
            new_quadrant=change.new_quadrant,
        )
        self.session.add(model)
        await self.session.flush()
