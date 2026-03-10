"""Async repository for company-related operations."""

from __future__ import annotations

import re
from typing import Any

import pandas as pd
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from ai_opportunity_index.domains import Company, CompanyVenture
from ai_opportunity_index.storage.models import (
    CompanyModel,
    CompanyScoreModel,
    CompanyVentureModel,
    EvidenceGroupModel,
    EvidenceModel,
    FinancialObservationModel,
    PipelineRunModel,
)
from ai_opportunity_index.storage.repositories.base import BaseRepository


def _slugify(name: str) -> str:
    """Convert a company name to a URL-safe slug (uppercase, max 50 chars)."""
    slug = re.sub(r"[^A-Za-z0-9]+", "-", name.strip()).strip("-").upper()
    return slug[:50] if slug else "UNKNOWN"


class CompanyRepository(BaseRepository[CompanyModel]):
    """Async repository for company CRUD and relationship queries."""

    model_class = CompanyModel

    @staticmethod
    def _to_domain(m: CompanyModel) -> Company:
        return Company(
            id=m.id,
            ticker=m.ticker,
            slug=m.slug,
            exchange=m.exchange,
            company_name=m.company_name,
            cik=m.cik,
            sic=m.sic,
            naics=m.naics,
            country=m.country,
            sector=m.sector,
            industry=m.industry,
            is_active=m.is_active,
            child_ticker_refs=m.child_ticker_refs,
            canonical_company_id=m.canonical_company_id,
            created_at=m.created_at,
            updated_at=m.updated_at,
        )

    async def upsert_company(self, data: Company | dict) -> Company:
        """Insert or update a company record."""
        if isinstance(data, dict):
            data = Company(**{k: v for k, v in data.items() if v is not None and k != "id"})

        existing = None
        if data.ticker:
            result = await self.session.execute(
                select(CompanyModel).where(CompanyModel.ticker == data.ticker)
            )
            existing = result.scalar_one_or_none()
        if existing is None and data.slug:
            result = await self.session.execute(
                select(CompanyModel).where(CompanyModel.slug == data.slug)
            )
            existing = result.scalar_one_or_none()

        if existing is None:
            slug = data.slug or (
                data.ticker.upper() if data.ticker else _slugify(data.company_name or "unknown")
            )
            model = CompanyModel(
                ticker=data.ticker,
                slug=slug,
                exchange=data.exchange,
                company_name=data.company_name,
                cik=data.cik,
                sic=data.sic,
                naics=data.naics,
                country=data.country,
                sector=data.sector,
                industry=data.industry,
                is_active=data.is_active,
            )
            self.session.add(model)
        else:
            model = existing
            for field in [
                "exchange", "company_name", "cik", "sic", "naics", "country",
                "sector", "industry", "is_active",
            ]:
                val = getattr(data, field)
                if val is not None:
                    setattr(model, field, val)

        await self.session.flush()
        return self._to_domain(model)

    async def upsert_company_in_session(self, data: dict) -> CompanyModel:
        """Insert or update a company within the current session (for bulk ops)."""
        ticker = data.get("ticker")
        existing = None
        if ticker:
            result = await self.session.execute(
                select(CompanyModel).where(CompanyModel.ticker == ticker)
            )
            existing = result.scalar_one_or_none()
        if existing is None and data.get("slug"):
            result = await self.session.execute(
                select(CompanyModel).where(CompanyModel.slug == data["slug"])
            )
            existing = result.scalar_one_or_none()

        if existing is None:
            if "slug" not in data or not data["slug"]:
                data["slug"] = (
                    ticker.upper() if ticker else _slugify(data.get("company_name", "unknown"))
                )
            model = CompanyModel(**{k: v for k, v in data.items() if k != "id"})
            self.session.add(model)
        else:
            model = existing
            for key, value in data.items():
                if key not in ("id", "slug") and value is not None:
                    setattr(model, key, value)
        return model

    async def upsert_companies_bulk(self, df: pd.DataFrame) -> int:
        """Insert or update companies from a DataFrame."""
        count = 0
        for _, row in df.iterrows():
            data = {k: v for k, v in row.to_dict().items() if pd.notna(v)}
            if "ticker" not in data:
                continue
            await self.upsert_company_in_session(data)
            count += 1
        await self.session.flush()
        return count

    async def get_company_by_ticker(
        self, ticker: str, resolve_alias: bool = True
    ) -> Company | None:
        """Look up a company by ticker (or slug fallback).

        If resolve_alias is True and the ticker is a child share class,
        returns the canonical (parent) company instead.
        """
        result = await self.session.execute(
            select(CompanyModel).where(CompanyModel.ticker == ticker.upper())
        )
        model = result.scalar_one_or_none()
        if not model:
            result = await self.session.execute(
                select(CompanyModel).where(CompanyModel.slug == ticker.upper())
            )
            model = result.scalar_one_or_none()
        if not model:
            return None
        if resolve_alias and model.canonical_company_id:
            parent = await self.session.get(CompanyModel, model.canonical_company_id)
            if parent:
                return self._to_domain(parent)
        return self._to_domain(model)

    async def get_company_model_by_ticker(
        self, ticker: str, resolve_alias: bool = True
    ) -> CompanyModel | None:
        """Look up a CompanyModel by ticker within the session.

        If resolve_alias is True and the ticker is a child share class,
        returns the canonical (parent) CompanyModel instead.
        """
        result = await self.session.execute(
            select(CompanyModel).where(CompanyModel.ticker == ticker.upper())
        )
        model = result.scalar_one_or_none()
        if not model:
            return None
        if resolve_alias and model.canonical_company_id:
            parent = await self.session.get(CompanyModel, model.canonical_company_id)
            if parent:
                return parent
        return model

    async def get_or_create_company_by_slug(
        self,
        slug: str,
        company_name: str,
        ticker: str | None = None,
        **extra_fields: Any,
    ) -> Company:
        """Find an existing company by slug or create a new one.

        Extra keyword arguments (e.g. sector, industry, exchange, country)
        are set on the model at creation time, and also used to fill in
        blank fields on an existing record.
        """
        ALLOWED_FIELDS = {
            "sector", "industry", "exchange", "country",
            "ir_url", "github_url", "careers_url", "blog_url",
        }
        filtered = {k: v for k, v in extra_fields.items() if k in ALLOWED_FIELDS and v}

        result = await self.session.execute(
            select(CompanyModel).where(CompanyModel.slug == slug)
        )
        model = result.scalar_one_or_none()

        if model is None:
            model = CompanyModel(
                ticker=ticker,
                slug=slug,
                company_name=company_name,
                is_active=True,
                **filtered,
            )
            self.session.add(model)
            await self.session.flush()
        else:
            changed = False
            if company_name and not model.company_name:
                model.company_name = company_name
                changed = True
            for attr, val in filtered.items():
                if not getattr(model, attr, None):
                    setattr(model, attr, val)
                    changed = True
            if changed:
                await self.session.flush()

        return self._to_domain(model)

    async def get_company_by_slug(self, slug: str) -> Company | None:
        """Look up a company by slug."""
        result = await self.session.execute(
            select(CompanyModel).where(CompanyModel.slug == slug.upper())
        )
        model = result.scalar_one_or_none()
        if not model:
            return None
        return self._to_domain(model)

    async def create_company_venture(
        self,
        parent_id: int,
        subsidiary_id: int,
        ownership_pct: float | None = None,
        relationship_type: str = "subsidiary",
        notes: str | None = None,
    ) -> CompanyVenture:
        """Create or update a venture relationship between parent and subsidiary."""
        result = await self.session.execute(
            select(CompanyVentureModel).where(
                CompanyVentureModel.parent_id == parent_id,
                CompanyVentureModel.subsidiary_id == subsidiary_id,
            )
        )
        existing = result.scalar_one_or_none()

        if existing:
            if ownership_pct is not None:
                existing.ownership_pct = ownership_pct
            if relationship_type:
                existing.relationship_type = relationship_type
            if notes is not None:
                existing.notes = notes
            model = existing
        else:
            model = CompanyVentureModel(
                parent_id=parent_id,
                subsidiary_id=subsidiary_id,
                ownership_pct=ownership_pct,
                relationship_type=relationship_type,
                notes=notes,
            )
            self.session.add(model)

        await self.session.flush()
        return CompanyVenture(
            id=model.id,
            parent_id=model.parent_id,
            subsidiary_id=model.subsidiary_id,
            ownership_pct=model.ownership_pct,
            relationship_type=model.relationship_type,
            notes=model.notes,
        )

    async def get_company_subsidiaries(self, company_id: int) -> list[dict]:
        """Get subsidiaries for a parent company with score status."""
        result = await self.session.execute(
            select(CompanyVentureModel).where(
                CompanyVentureModel.parent_id == company_id
            )
        )
        ventures = result.scalars().all()

        results: list[dict] = []
        for v in ventures:
            sub = await self.session.get(CompanyModel, v.subsidiary_id)
            if not sub:
                continue
            score_check = await self.session.execute(
                select(CompanyScoreModel.id).where(
                    CompanyScoreModel.company_id == v.subsidiary_id
                ).limit(1)
            )
            has_scores = score_check.scalar_one_or_none() is not None
            latest_score = None
            if has_scores:
                score_result = await self.session.execute(
                    select(CompanyScoreModel).where(
                        CompanyScoreModel.company_id == v.subsidiary_id
                    ).order_by(CompanyScoreModel.scored_at.desc()).limit(1)
                )
                score = score_result.scalar_one_or_none()
                if score:
                    latest_score = {
                        "opportunity": score.opportunity,
                        "realization": score.realization,
                    }
            results.append({
                "id": sub.id,
                "ticker": sub.ticker,
                "slug": sub.slug,
                "company_name": sub.company_name,
                "ownership_pct": v.ownership_pct,
                "relationship_type": v.relationship_type,
                "has_scores": has_scores,
                "latest_score": latest_score,
            })
        return results

    async def get_company_parents(self, company_id: int) -> list[dict]:
        """Get parent companies for a subsidiary."""
        result = await self.session.execute(
            select(CompanyVentureModel).where(
                CompanyVentureModel.subsidiary_id == company_id
            )
        )
        ventures = result.scalars().all()

        results: list[dict] = []
        for v in ventures:
            parent = await self.session.get(CompanyModel, v.parent_id)
            if not parent:
                continue
            results.append({
                "id": parent.id,
                "ticker": parent.ticker,
                "slug": parent.slug,
                "company_name": parent.company_name,
                "ownership_pct": v.ownership_pct,
                "relationship_type": v.relationship_type,
            })
        return results

    async def get_company_detail(self, ticker: str) -> dict | None:
        """Get full detail for a single company including latest scores and evidence.

        If the ticker is a child share class, resolves to the canonical parent and
        combines evidence and financials from parent + children.
        Each evidence item is tagged with ``source_ticker`` so the frontend can
        show which listing the evidence originated from.
        """
        from sqlalchemy import func as sa_func

        requested_ticker = ticker.upper()

        result = await self.session.execute(
            select(CompanyModel).where(CompanyModel.ticker == requested_ticker)
        )
        company = result.scalar_one_or_none()
        if not company:
            result = await self.session.execute(
                select(CompanyModel).where(CompanyModel.slug == requested_ticker)
            )
            company = result.scalar_one_or_none()
        if not company:
            return None

        is_alias = bool(company.canonical_company_id)
        if company.canonical_company_id:
            parent = await self.session.get(CompanyModel, company.canonical_company_id)
            if parent:
                company = parent

        # Build list of company_ids to query (parent + children)
        company_ids = [company.id]
        id_to_ticker: dict[int, str] = {company.id: company.ticker}
        child_tickers: list[str] = []
        if company.child_ticker_refs:
            for child_id in company.child_ticker_refs:
                child = await self.session.get(CompanyModel, child_id)
                if child:
                    company_ids.append(child_id)
                    child_tickers.append(child.ticker)
                    id_to_ticker[child_id] = child.ticker

        # Latest score (from canonical company)
        score_result = await self.session.execute(
            select(CompanyScoreModel)
            .where(CompanyScoreModel.company_id == company.id)
            .order_by(CompanyScoreModel.scored_at.desc())
            .limit(1)
        )
        score = score_result.scalar_one_or_none()

        # All evidence from parent + children
        ev_result = await self.session.execute(
            select(EvidenceModel)
            .where(EvidenceModel.company_id.in_(company_ids))
            .order_by(EvidenceModel.observed_at.desc())
        )
        evidence_models = ev_result.scalars().all()

        evidence_by_type: dict[str, list] = {}
        for ev in evidence_models:
            evidence_by_type.setdefault(ev.evidence_type, []).append({
                "id": ev.id,
                "subtype": ev.evidence_subtype,
                "source_name": ev.source_name,
                "source_url": ev.source_url,
                "source_date": str(ev.source_date) if ev.source_date else None,
                "score_contribution": ev.score_contribution,
                "weight": ev.weight,
                "signal_strength": ev.signal_strength,
                "target_dimension": ev.target_dimension,
                "capture_stage": ev.capture_stage,
                "source_excerpt": ev.source_excerpt,
                "dollar_estimate_usd": ev.dollar_estimate_usd,
                "dollar_year_1": ev.dollar_year_1,
                "dollar_year_2": ev.dollar_year_2,
                "dollar_year_3": ev.dollar_year_3,
                "payload": ev.payload,
                "valid_from": str(ev.valid_from) if ev.valid_from else None,
                "valid_to": str(ev.valid_to) if ev.valid_to else None,
                "pipeline_run_id": ev.pipeline_run_id,
                "source_ticker": id_to_ticker.get(ev.company_id, company.ticker),
            })

        # Fetch latest financial observations -- prefer parent, fill gaps from children
        latest_financials = await self._get_latest_financials_in_session(company.id)
        for child_id in company_ids[1:]:
            child_financials = await self._get_latest_financials_in_session(child_id)
            for metric, obs_model in child_financials.items():
                if metric not in latest_financials:
                    latest_financials[metric] = obs_model

        financials_dict: dict[str, dict] = {}
        for metric, obs_model in latest_financials.items():
            financials_dict[metric] = {
                "value": obs_model.value,
                "value_units": obs_model.value_units,
                "source_datetime": str(obs_model.source_datetime) if obs_model.source_datetime else None,
                "source_name": obs_model.source_name,
                "source_link": obs_model.source_link,
                "fiscal_period": obs_model.fiscal_period,
            }

        # Query subsidiaries with pipeline completion status
        subsidiaries: list[dict] = []
        vent_result = await self.session.execute(
            select(CompanyVentureModel).where(
                CompanyVentureModel.parent_id == company.id
            )
        )
        ventures = vent_result.scalars().all()
        for v in ventures:
            sub = await self.session.get(CompanyModel, v.subsidiary_id)
            if not sub:
                continue

            sub_score_result = await self.session.execute(
                select(CompanyScoreModel).where(
                    CompanyScoreModel.company_id == v.subsidiary_id
                ).order_by(CompanyScoreModel.scored_at.desc()).limit(1)
            )
            sub_score = sub_score_result.scalar_one_or_none()

            has_links = bool(sub.github_url or sub.careers_url or sub.ir_url or sub.blog_url)

            ev_check = await self.session.execute(
                select(EvidenceModel.id).where(
                    EvidenceModel.company_id == v.subsidiary_id
                ).limit(1)
            )
            has_evidence = ev_check.scalar_one_or_none() is not None

            eg_check = await self.session.execute(
                select(EvidenceGroupModel.id).where(
                    EvidenceGroupModel.company_id == v.subsidiary_id
                ).limit(1)
            )
            has_groups = eg_check.scalar_one_or_none() is not None

            has_scores = sub_score is not None

            milestones_done = sum([has_links, has_evidence, has_groups, has_scores])
            pipeline_pct = round(milestones_done / 4 * 100)

            last_run = None
            if sub.ticker or sub.slug:
                run_result = await self.session.execute(
                    select(PipelineRunModel.completed_at).where(
                        PipelineRunModel.tickers_requested.any(sub.ticker or sub.slug)
                    ).order_by(PipelineRunModel.completed_at.desc()).limit(1)
                )
                last_run = run_result.scalar_one_or_none()
            last_checked = last_run or sub.updated_at

            subsidiaries.append({
                "id": sub.id,
                "ticker": sub.ticker,
                "slug": sub.slug,
                "company_name": sub.company_name,
                "sector": sub.sector,
                "industry": sub.industry,
                "ownership_pct": v.ownership_pct,
                "relationship_type": v.relationship_type,
                "has_links": has_links,
                "has_evidence": has_evidence,
                "has_groups": has_groups,
                "has_scores": has_scores,
                "pipeline_pct": pipeline_pct,
                "last_checked": str(last_checked) if last_checked else None,
                "created_at": str(sub.created_at) if sub.created_at else None,
                "latest_score": {
                    "opportunity": sub_score.opportunity,
                    "realization": sub_score.realization,
                    "quadrant_label": sub_score.quadrant_label,
                } if sub_score else None,
            })

        # Query parent companies (if this is a subsidiary)
        parents: list[dict] = []
        pv_result = await self.session.execute(
            select(CompanyVentureModel).where(
                CompanyVentureModel.subsidiary_id == company.id
            )
        )
        parent_ventures = pv_result.scalars().all()
        for v in parent_ventures:
            p = await self.session.get(CompanyModel, v.parent_id)
            if not p:
                continue
            parents.append({
                "id": p.id,
                "ticker": p.ticker,
                "slug": p.slug,
                "company_name": p.company_name,
                "ownership_pct": v.ownership_pct,
                "relationship_type": v.relationship_type,
            })

        result_dict = {
            "ticker": company.ticker,
            "slug": company.slug,
            "requested_ticker": requested_ticker,
            "is_alias": is_alias,
            "canonical_ticker": company.ticker if is_alias else None,
            "company_name": company.company_name,
            "exchange": company.exchange,
            "sic": company.sic,
            "sector": company.sector,
            "industry": company.industry,
            "child_tickers": child_tickers,
            "subsidiaries": subsidiaries,
            "parent_companies": parents,
            "financials": financials_dict,
            "scores": {
                "revenue_opp": score.revenue_opp_score if score else None,
                "cost_opp": score.cost_opp_score if score else None,
                "composite_opp": score.composite_opp_score if score else None,
                "filing_nlp": score.filing_nlp_score if score else None,
                "product": score.product_score if score else None,
                "github": score.github_score if score else None,
                "analyst": score.analyst_score if score else None,
                "composite_real": score.composite_real_score if score else None,
                "cost_capture": score.cost_capture_score if score else None,
                "revenue_capture": score.revenue_capture_score if score else None,
                "general_investment": score.general_investment_score if score else None,
                "cost_roi": score.cost_roi if score else None,
                "revenue_roi": score.revenue_roi if score else None,
                "combined_roi": score.combined_roi if score else None,
                "opportunity": score.opportunity if score else None,
                "realization": score.realization if score else None,
                "quadrant": score.quadrant if score else None,
                "quadrant_label": score.quadrant_label if score else None,
                "cost_opp_usd": score.cost_opp_usd if score else None,
                "revenue_opp_usd": score.revenue_opp_usd if score else None,
                "cost_capture_usd": score.cost_capture_usd if score else None,
                "revenue_capture_usd": score.revenue_capture_usd if score else None,
                "total_investment_usd": score.total_investment_usd if score else None,
                "ai_index_usd": score.ai_index_usd if score else None,
                "capture_probability": score.capture_probability if score else None,
                "opportunity_usd": score.opportunity_usd if score else None,
                "evidence_dollars": score.evidence_dollars if score else None,
                "data_as_of": str(score.data_as_of) if score else None,
                "scored_at": str(score.scored_at) if score else None,
            },
            "flags": score.flags if score else [],
            "evidence": evidence_by_type,
        }
        return result_dict

    async def get_ai_index_rank(self, ticker: str) -> dict:
        """Compute the AI Index rank for a company across the scored universe.

        Returns {"rank": N, "total": M} or {"rank": None, "total": 0}.
        """
        result = await self.session.execute(
            text(
                "SELECT ticker, ai_index_usd FROM latest_company_scores "
                "WHERE ai_index_usd IS NOT NULL "
                "ORDER BY ai_index_usd DESC"
            )
        )
        rows = result.fetchall()
        if not rows:
            return {"rank": None, "total": 0}

        ticker_upper = ticker.upper()
        for i, row in enumerate(rows):
            if row[0] == ticker_upper:
                return {"rank": i + 1, "total": len(rows)}

        return {"rank": None, "total": len(rows)}

    async def get_industry_peers(self, ticker: str, limit: int = 20) -> list[dict]:
        """Get companies in the same industry as the given ticker for peer comparison."""
        result = await self.session.execute(
            select(CompanyModel).where(CompanyModel.ticker == ticker.upper())
        )
        company = result.scalar_one_or_none()

        if not company or not company.industry:
            return []

        peer_result = await self.session.execute(
            text(
                "SELECT * FROM latest_company_scores WHERE industry = :industry "
                "ORDER BY opportunity DESC LIMIT :limit"
            ),
            {"industry": company.industry, "limit": limit},
        )
        columns = peer_result.keys()
        return [dict(zip(columns, row)) for row in peer_result.fetchall()]

    async def _get_latest_financials_in_session(
        self, company_id: int
    ) -> dict[str, FinancialObservationModel]:
        """Internal helper: get latest financial observation models."""
        from sqlalchemy import func as sa_func

        latest_sub = (
            select(
                FinancialObservationModel.metric,
                sa_func.max(FinancialObservationModel.source_datetime).label("max_dt"),
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
        return {m.metric: m for m in models}
