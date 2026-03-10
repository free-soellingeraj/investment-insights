"""Async repository for financial observation operations."""

from __future__ import annotations

from sqlalchemy import func, select

from ai_opportunity_index.domains import FinancialObservation
from ai_opportunity_index.storage.models import FinancialObservationModel
from ai_opportunity_index.storage.repositories.base import BaseRepository


class FinancialObservationRepository(BaseRepository[FinancialObservationModel]):
    """Async repository for financial observation CRUD."""

    model_class = FinancialObservationModel

    @staticmethod
    def _to_domain(m: FinancialObservationModel) -> FinancialObservation:
        return FinancialObservation(
            id=m.id,
            company_id=m.company_id,
            metric=m.metric,
            value=m.value,
            value_units=m.value_units,
            source_datetime=m.source_datetime,
            source_link=m.source_link,
            source_name=m.source_name,
            fiscal_period=m.fiscal_period,
            created_at=m.created_at,
        )

    @staticmethod
    def _to_model(obs: FinancialObservation) -> FinancialObservationModel:
        return FinancialObservationModel(
            company_id=obs.company_id,
            metric=obs.metric,
            value=obs.value,
            value_units=obs.value_units,
            source_datetime=obs.source_datetime,
            source_link=obs.source_link,
            source_name=obs.source_name,
            fiscal_period=obs.fiscal_period,
        )

    async def save_financial_observation(
        self, obs: FinancialObservation
    ) -> FinancialObservation:
        """Save a single financial observation."""
        model = self._to_model(obs)
        self.session.add(model)
        await self.session.flush()
        return self._to_domain(model)

    async def save_financial_observations_batch(
        self, items: list[FinancialObservation]
    ) -> None:
        """Bulk insert financial observation rows."""
        for obs in items:
            model = self._to_model(obs)
            self.session.add(model)
        await self.session.flush()

    async def get_latest_financial(
        self, company_id: int, metric: str
    ) -> FinancialObservation | None:
        """Get the most recent observation for a company+metric."""
        result = await self.session.execute(
            select(FinancialObservationModel)
            .where(
                FinancialObservationModel.company_id == company_id,
                FinancialObservationModel.metric == metric,
            )
            .order_by(FinancialObservationModel.source_datetime.desc())
            .limit(1)
        )
        model = result.scalar_one_or_none()
        return self._to_domain(model) if model else None

    async def get_latest_financials(
        self, company_id: int
    ) -> dict[str, FinancialObservation]:
        """Get the latest observation per metric for a company.

        Returns {'market_cap': obs, 'revenue': obs, ...}.
        """
        latest_sub = (
            select(
                FinancialObservationModel.metric,
                func.max(FinancialObservationModel.source_datetime).label("max_dt"),
            )
            .where(FinancialObservationModel.company_id == company_id)
            .group_by(FinancialObservationModel.metric)
            .subquery()
        )

        result = await self.session.execute(
            select(FinancialObservationModel)
            .join(
                latest_sub,
                (FinancialObservationModel.metric == latest_sub.c.metric)
                & (FinancialObservationModel.source_datetime == latest_sub.c.max_dt),
            )
            .where(FinancialObservationModel.company_id == company_id)
        )
        models = result.scalars().all()
        return {m.metric: self._to_domain(m) for m in models}

    async def get_financial_history(
        self, company_id: int, metric: str, limit: int = 20
    ) -> list[FinancialObservation]:
        """Get historical observations for a company+metric."""
        result = await self.session.execute(
            select(FinancialObservationModel)
            .where(
                FinancialObservationModel.company_id == company_id,
                FinancialObservationModel.metric == metric,
            )
            .order_by(FinancialObservationModel.source_datetime.desc())
            .limit(limit)
        )
        models = result.scalars().all()
        return [self._to_domain(m) for m in models]
