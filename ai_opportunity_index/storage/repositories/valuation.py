"""Async repository for valuation-related operations (groups, passages, valuations, discrepancies)."""

from __future__ import annotations

from sqlalchemy import delete, select
from sqlalchemy.orm import selectinload

from ai_opportunity_index.domains import (
    CaptureDetails,
    EvidenceGroup,
    EvidenceGroupPassage,
    InvestmentDetails,
    PlanDetails,
    SynthesizedProject,
    Valuation,
    ValuationDiscrepancy,
    ValuationStage,
)
from ai_opportunity_index.storage.models import (
    CaptureDetailModel,
    CompanyModel,
    EvidenceGroupModel,
    EvidenceGroupPassageModel,
    InvestmentDetailModel,
    InvestmentProjectModel,
    PlanDetailModel,
    ValuationDiscrepancyModel,
    ValuationModel,
)
from ai_opportunity_index.storage.repositories.base import BaseRepository


class ValuationRepository(BaseRepository[ValuationModel]):
    """Async repository for evidence groups, valuations, and discrepancies."""

    model_class = ValuationModel

    @staticmethod
    def _valuation_to_domain(m: ValuationModel) -> Valuation:
        """Convert a ValuationModel to a Valuation domain model."""
        plan_detail = None
        investment_detail = None
        capture_detail = None

        if m.evidence_type == "plan" and m.plan_detail:
            d = m.plan_detail
            plan_detail = PlanDetails(
                timeframe=d.timeframe or "",
                probability=d.probability or 0.5,
                strategic_rationale=d.strategic_rationale or "",
                contingencies=d.contingencies or "",
                horizon_shape=d.horizon_shape or "s_curve",
                year_1_pct=d.year_1_pct or 0.15,
                year_2_pct=d.year_2_pct or 0.60,
                year_3_pct=d.year_3_pct or 1.0,
            )
        elif m.evidence_type == "investment" and m.investment_detail:
            d = m.investment_detail
            investment_detail = InvestmentDetails(
                actual_spend_usd=d.actual_spend_usd,
                deployment_scope=d.deployment_scope or "",
                completion_pct=d.completion_pct or 0.5,
                technology_area=d.technology_area or "",
                vendor_partner=d.vendor_partner or "",
                horizon_shape=d.horizon_shape or "linear_ramp",
                year_1_pct=d.year_1_pct or 0.33,
                year_2_pct=d.year_2_pct or 0.66,
                year_3_pct=d.year_3_pct or 1.0,
            )
        elif m.evidence_type == "capture" and m.capture_detail:
            d = m.capture_detail
            capture_detail = CaptureDetails(
                metric_name=d.metric_name or "",
                metric_value_before=d.metric_value_before or "",
                metric_value_after=d.metric_value_after or "",
                metric_delta=d.metric_delta or "",
                measurement_period=d.measurement_period or "",
                measured_dollar_impact=d.measured_dollar_impact,
                horizon_shape=d.horizon_shape or "flat",
                year_1_pct=d.year_1_pct if d.year_1_pct is not None else 1.0,
                year_2_pct=d.year_2_pct if d.year_2_pct is not None else 1.0,
                year_3_pct=d.year_3_pct if d.year_3_pct is not None else 1.0,
            )

        return Valuation(
            id=m.id,
            group_id=m.group_id,
            pipeline_run_id=m.pipeline_run_id,
            stage=m.stage,
            preliminary_id=m.preliminary_id,
            evidence_type=m.evidence_type,
            narrative=m.narrative,
            confidence=m.confidence,
            dollar_low=m.dollar_low,
            dollar_high=m.dollar_high,
            dollar_mid=m.dollar_mid,
            dollar_rationale=m.dollar_rationale or "",
            specificity=m.specificity,
            magnitude=m.magnitude,
            stage_weight=m.stage_weight,
            recency=m.recency,
            factor_score=m.factor_score,
            adjusted_from_preliminary=m.adjusted_from_preliminary,
            adjustment_reason=m.adjustment_reason,
            prior_groups_seen=m.prior_groups_seen or 0,
            input_tokens=m.input_tokens or 0,
            output_tokens=m.output_tokens or 0,
            model_name=m.model_name,
            plan_detail=plan_detail,
            investment_detail=investment_detail,
            capture_detail=capture_detail,
        )

    async def save_evidence_group(self, group: EvidenceGroup) -> EvidenceGroup:
        """Save an evidence group and its passages. Returns domain model with id set."""
        m = EvidenceGroupModel(
            company_id=group.company_id,
            pipeline_run_id=group.pipeline_run_id,
            target_dimension=group.target_dimension,
            evidence_type=group.evidence_type,
            passage_count=group.passage_count,
            source_types=group.source_types,
            date_earliest=group.date_earliest,
            date_latest=group.date_latest,
            mean_confidence=group.mean_confidence,
            max_confidence=group.max_confidence,
            representative_text=group.representative_text,
        )
        self.session.add(m)
        await self.session.flush()

        for p in group.passages:
            pm = EvidenceGroupPassageModel(
                group_id=m.id,
                evidence_id=p.evidence_id,
                passage_text=p.passage_text,
                source_type=p.source_type,
                source_filename=p.source_filename,
                source_date=p.source_date,
                confidence=p.confidence,
                reasoning=p.reasoning,
                target_dimension=p.target_dimension,
                capture_stage=p.capture_stage,
                source_url=p.source_url,
                source_author=p.source_author,
                source_author_role=p.source_author_role,
                source_author_affiliation=p.source_author_affiliation,
                source_publisher=p.source_publisher,
                source_access_date=p.source_access_date,
                source_authority=p.source_authority.value if p.source_authority else None,
            )
            self.session.add(pm)

        await self.session.flush()
        group.id = m.id
        return group

    async def save_valuation(self, val: Valuation) -> Valuation:
        """Save a valuation and its type-specific detail. Returns domain model with id set."""
        # Compute dollar_mid
        dollar_mid = None
        if val.dollar_low is not None and val.dollar_high is not None:
            dollar_mid = (val.dollar_low + val.dollar_high) / 2.0
        elif val.dollar_mid is not None:
            dollar_mid = val.dollar_mid

        m = ValuationModel(
            group_id=val.group_id,
            pipeline_run_id=val.pipeline_run_id,
            stage=val.stage,
            preliminary_id=val.preliminary_id,
            evidence_type=val.evidence_type,
            narrative=val.narrative,
            confidence=val.confidence,
            dollar_low=val.dollar_low,
            dollar_high=val.dollar_high,
            dollar_mid=dollar_mid,
            dollar_rationale=val.dollar_rationale,
            specificity=val.specificity,
            magnitude=val.magnitude,
            stage_weight=val.stage_weight,
            recency=val.recency,
            factor_score=val.factor_score,
            adjusted_from_preliminary=val.adjusted_from_preliminary,
            adjustment_reason=val.adjustment_reason,
            prior_groups_seen=val.prior_groups_seen,
            input_tokens=val.input_tokens,
            output_tokens=val.output_tokens,
            model_name=val.model_name,
        )
        self.session.add(m)
        await self.session.flush()

        # Save type-specific detail
        if val.evidence_type == "plan" and val.plan_detail:
            d = val.plan_detail
            self.session.add(PlanDetailModel(
                valuation_id=m.id,
                timeframe=d.timeframe,
                probability=d.probability,
                strategic_rationale=d.strategic_rationale,
                contingencies=d.contingencies,
                horizon_shape=d.horizon_shape,
                year_1_pct=d.year_1_pct,
                year_2_pct=d.year_2_pct,
                year_3_pct=d.year_3_pct,
            ))
        elif val.evidence_type == "investment" and val.investment_detail:
            d = val.investment_detail
            self.session.add(InvestmentDetailModel(
                valuation_id=m.id,
                actual_spend_usd=d.actual_spend_usd,
                deployment_scope=d.deployment_scope,
                completion_pct=d.completion_pct,
                technology_area=d.technology_area,
                vendor_partner=d.vendor_partner,
                horizon_shape=d.horizon_shape,
                year_1_pct=d.year_1_pct,
                year_2_pct=d.year_2_pct,
                year_3_pct=d.year_3_pct,
            ))
        elif val.evidence_type == "capture" and val.capture_detail:
            d = val.capture_detail
            self.session.add(CaptureDetailModel(
                valuation_id=m.id,
                metric_name=d.metric_name,
                metric_value_before=d.metric_value_before,
                metric_value_after=d.metric_value_after,
                metric_delta=d.metric_delta,
                measurement_period=d.measurement_period,
                measured_dollar_impact=d.measured_dollar_impact,
                horizon_shape=d.horizon_shape,
                year_1_pct=d.year_1_pct,
                year_2_pct=d.year_2_pct,
                year_3_pct=d.year_3_pct,
            ))

        await self.session.flush()
        val.id = m.id
        val.dollar_mid = dollar_mid
        return val

    async def save_valuation_discrepancy(
        self, disc: ValuationDiscrepancy
    ) -> ValuationDiscrepancy:
        """Save a valuation discrepancy record."""
        m = ValuationDiscrepancyModel(
            company_id=disc.company_id,
            pipeline_run_id=disc.pipeline_run_id,
            group_id_a=disc.group_id_a,
            group_id_b=disc.group_id_b,
            description=disc.description,
            resolution=disc.resolution,
            resolution_method=disc.resolution_method,
            source_search_result=disc.source_search_result,
            trusted_group_id=disc.trusted_group_id,
        )
        self.session.add(m)
        await self.session.flush()
        disc.id = m.id
        return disc

    async def get_evidence_groups_for_company(
        self,
        company_id: int,
        pipeline_run_id: int | None = None,
    ) -> list[EvidenceGroup]:
        """Get all evidence groups for a company, optionally filtered by pipeline run."""
        stmt = (
            select(EvidenceGroupModel)
            .where(EvidenceGroupModel.company_id == company_id)
        )
        if pipeline_run_id is not None:
            stmt = stmt.where(EvidenceGroupModel.pipeline_run_id == pipeline_run_id)
        stmt = stmt.order_by(EvidenceGroupModel.id)

        result = await self.session.execute(stmt)
        group_models = result.scalars().all()

        groups: list[EvidenceGroup] = []
        for m in group_models:
            # Load passages for this group
            p_result = await self.session.execute(
                select(EvidenceGroupPassageModel).where(
                    EvidenceGroupPassageModel.group_id == m.id
                )
            )
            passage_models = p_result.scalars().all()

            passages = [
                EvidenceGroupPassage(
                    id=p.id,
                    group_id=p.group_id,
                    evidence_id=p.evidence_id,
                    passage_text=p.passage_text,
                    source_type=p.source_type,
                    source_filename=p.source_filename,
                    source_date=p.source_date,
                    confidence=p.confidence,
                    reasoning=p.reasoning,
                    target_dimension=p.target_dimension,
                    capture_stage=p.capture_stage,
                    source_url=p.source_url,
                    source_author=p.source_author,
                    source_author_role=p.source_author_role,
                    source_author_affiliation=p.source_author_affiliation,
                    source_publisher=p.source_publisher,
                    source_access_date=p.source_access_date,
                    source_authority=p.source_authority,
                )
                for p in passage_models
            ]
            groups.append(EvidenceGroup(
                id=m.id,
                company_id=m.company_id,
                pipeline_run_id=m.pipeline_run_id,
                target_dimension=m.target_dimension,
                evidence_type=m.evidence_type,
                passage_count=m.passage_count,
                source_types=m.source_types or [],
                date_earliest=m.date_earliest,
                date_latest=m.date_latest,
                mean_confidence=m.mean_confidence,
                max_confidence=m.max_confidence,
                representative_text=m.representative_text,
                passages=passages,
            ))
        return groups

    async def get_final_valuations_for_company(
        self,
        company_id: int,
        pipeline_run_id: int | None = None,
    ) -> list[Valuation]:
        """Get all final-stage valuations for a company."""
        stmt = (
            select(ValuationModel)
            .join(EvidenceGroupModel)
            .where(
                EvidenceGroupModel.company_id == company_id,
                ValuationModel.stage == ValuationStage.FINAL,
            )
            .options(
                selectinload(ValuationModel.plan_detail),
                selectinload(ValuationModel.investment_detail),
                selectinload(ValuationModel.capture_detail),
            )
        )
        if pipeline_run_id is not None:
            stmt = stmt.where(ValuationModel.pipeline_run_id == pipeline_run_id)
        stmt = stmt.order_by(ValuationModel.id)

        result = await self.session.execute(stmt)
        models = result.scalars().all()
        return [self._valuation_to_domain(m) for m in models]

    async def delete_evidence_groups_for_company(
        self,
        company_id: int,
        pipeline_run_id: int | None = None,
    ) -> int:
        """Delete evidence groups (and cascading valuations/passages) for a company.

        Deletes valuation_discrepancies first since they reference evidence_groups
        without ON DELETE CASCADE.

        Returns the number of groups deleted.
        """
        # Delete discrepancies first (they reference groups without CASCADE)
        disc_stmt = delete(ValuationDiscrepancyModel).where(
            ValuationDiscrepancyModel.company_id == company_id
        )
        if pipeline_run_id is not None:
            disc_stmt = disc_stmt.where(
                ValuationDiscrepancyModel.pipeline_run_id == pipeline_run_id
            )
        await self.session.execute(disc_stmt)

        # Now delete groups (valuations + passages cascade via ON DELETE CASCADE)
        group_stmt = delete(EvidenceGroupModel).where(
            EvidenceGroupModel.company_id == company_id
        )
        if pipeline_run_id is not None:
            group_stmt = group_stmt.where(
                EvidenceGroupModel.pipeline_run_id == pipeline_run_id
            )
        result = await self.session.execute(group_stmt)
        await self.session.flush()
        return result.rowcount

    async def get_company_valuation_detail(self, ticker: str) -> dict | None:
        """Get structured valuation data for a company's evidence viewer.

        Returns dict organized by dimension -> groups -> valuation + passages,
        with dimension aggregates and pipeline summary counts.
        Resolves child share-class aliases to the canonical parent.
        """
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

        # Build child tickers list
        child_tickers: list[str] = []
        if company.child_ticker_refs:
            for child_id in company.child_ticker_refs:
                child = await self.session.get(CompanyModel, child_id)
                if child:
                    child_tickers.append(child.ticker)

        # Load evidence groups with passages
        g_result = await self.session.execute(
            select(EvidenceGroupModel)
            .where(EvidenceGroupModel.company_id == company.id)
            .order_by(EvidenceGroupModel.id)
        )
        group_models = g_result.scalars().all()
        if not group_models:
            return None

        # Load final valuations keyed by group_id
        v_result = await self.session.execute(
            select(ValuationModel)
            .join(EvidenceGroupModel)
            .where(
                EvidenceGroupModel.company_id == company.id,
                ValuationModel.stage == ValuationStage.FINAL,
            )
            .options(
                selectinload(ValuationModel.plan_detail),
                selectinload(ValuationModel.investment_detail),
                selectinload(ValuationModel.capture_detail),
            )
        )
        val_models = v_result.scalars().all()
        val_by_group: dict[int, ValuationModel] = {}
        for vm in val_models:
            val_by_group[vm.group_id] = vm

        # Load discrepancies
        d_result = await self.session.execute(
            select(ValuationDiscrepancyModel).where(
                ValuationDiscrepancyModel.company_id == company.id
            )
        )
        disc_models = d_result.scalars().all()
        disc_by_group: dict[int, list] = {}
        for dm in disc_models:
            for gid in (dm.group_id_a, dm.group_id_b):
                disc_by_group.setdefault(gid, []).append({
                    "id": dm.id,
                    "group_id_a": dm.group_id_a,
                    "group_id_b": dm.group_id_b,
                    "description": dm.description,
                    "resolution": dm.resolution,
                    "resolution_method": dm.resolution_method,
                    "trusted_group_id": dm.trusted_group_id,
                })

        # Build structured output by dimension
        dimensions: dict[str, dict] = {}
        total_passages = 0
        total_groups = 0
        type_counts = {"plan": 0, "investment": 0, "capture": 0}

        for gm in group_models:
            dim = gm.target_dimension or "general"
            if dim not in dimensions:
                dimensions[dim] = {
                    "groups": [],
                    "raw_sum": 0.0,
                    "potential_usd": 0.0,
                    "actual_usd": 0.0,
                }

            total_groups += 1
            total_passages += gm.passage_count or 0

            # Load passages for this group
            p_result = await self.session.execute(
                select(EvidenceGroupPassageModel).where(
                    EvidenceGroupPassageModel.group_id == gm.id
                )
            )
            passage_models = p_result.scalars().all()

            passages = []
            for p in passage_models:
                passages.append({
                    "id": p.id,
                    "passage_text": p.passage_text,
                    "source_type": p.source_type,
                    "source_filename": p.source_filename,
                    "source_date": str(p.source_date) if p.source_date else None,
                    "confidence": p.confidence,
                    "reasoning": p.reasoning,
                    "target_dimension": p.target_dimension,
                    "capture_stage": p.capture_stage,
                    "source_url": p.source_url,
                    "source_author": p.source_author,
                    "source_author_role": p.source_author_role,
                    "source_author_affiliation": p.source_author_affiliation,
                    "source_publisher": p.source_publisher,
                    "source_access_date": str(p.source_access_date) if p.source_access_date else None,
                    "source_authority": p.source_authority,
                    "scraped_at": p.created_at.isoformat() if p.created_at else None,
                })

            # Valuation for this group
            vm = val_by_group.get(gm.id)
            valuation = None
            if vm:
                ev_type = vm.evidence_type or ""
                if ev_type in type_counts:
                    type_counts[ev_type] += 1

                # Type-specific detail
                type_detail = None
                if vm.evidence_type == "plan" and vm.plan_detail:
                    d = vm.plan_detail
                    type_detail = {
                        "timeframe": d.timeframe,
                        "probability": d.probability,
                        "strategic_rationale": d.strategic_rationale,
                        "contingencies": d.contingencies,
                        "horizon_shape": d.horizon_shape,
                        "year_1_pct": d.year_1_pct,
                        "year_2_pct": d.year_2_pct,
                        "year_3_pct": d.year_3_pct,
                    }
                elif vm.evidence_type == "investment" and vm.investment_detail:
                    d = vm.investment_detail
                    type_detail = {
                        "actual_spend_usd": d.actual_spend_usd,
                        "deployment_scope": d.deployment_scope,
                        "completion_pct": d.completion_pct,
                        "technology_area": d.technology_area,
                        "vendor_partner": d.vendor_partner,
                        "horizon_shape": d.horizon_shape,
                        "year_1_pct": d.year_1_pct,
                        "year_2_pct": d.year_2_pct,
                        "year_3_pct": d.year_3_pct,
                    }
                elif vm.evidence_type == "capture" and vm.capture_detail:
                    d = vm.capture_detail
                    type_detail = {
                        "metric_name": d.metric_name,
                        "metric_value_before": d.metric_value_before,
                        "metric_value_after": d.metric_value_after,
                        "metric_delta": d.metric_delta,
                        "measurement_period": d.measurement_period,
                        "measured_dollar_impact": d.measured_dollar_impact,
                        "horizon_shape": d.horizon_shape,
                        "year_1_pct": d.year_1_pct,
                        "year_2_pct": d.year_2_pct,
                        "year_3_pct": d.year_3_pct,
                    }

                valuation = {
                    "id": vm.id,
                    "evidence_type": vm.evidence_type,
                    "valued_at": vm.created_at.isoformat() if vm.created_at else None,
                    "narrative": vm.narrative,
                    "confidence": vm.confidence,
                    "dollar_low": vm.dollar_low,
                    "dollar_high": vm.dollar_high,
                    "dollar_mid": vm.dollar_mid,
                    "dollar_rationale": vm.dollar_rationale,
                    "specificity": vm.specificity,
                    "magnitude": vm.magnitude,
                    "stage_weight": vm.stage_weight,
                    "recency": vm.recency,
                    "factor_score": vm.factor_score,
                    "adjusted_from_preliminary": vm.adjusted_from_preliminary,
                    "adjustment_reason": vm.adjustment_reason,
                    "type_detail": type_detail,
                }

                # Aggregate into dimension
                if vm.factor_score is not None:
                    dimensions[dim]["raw_sum"] += vm.factor_score
                if vm.dollar_high is not None:
                    dimensions[dim]["potential_usd"] += vm.dollar_high
                if vm.dollar_mid is not None:
                    dimensions[dim]["actual_usd"] += vm.dollar_mid

            # Discrepancies for this group
            discrepancies = disc_by_group.get(gm.id, [])

            group_dict = {
                "id": gm.id,
                "target_dimension": dim,
                "evidence_type": gm.evidence_type,
                "passage_count": gm.passage_count,
                "source_types": gm.source_types or [],
                "date_earliest": str(gm.date_earliest) if gm.date_earliest else None,
                "date_latest": str(gm.date_latest) if gm.date_latest else None,
                "mean_confidence": gm.mean_confidence,
                "max_confidence": gm.max_confidence,
                "representative_text": gm.representative_text,
                "grouped_at": gm.created_at.isoformat() if gm.created_at else None,
                "valuation": valuation,
                "passages": passages,
                "discrepancies": discrepancies,
            }
            dimensions[dim]["groups"].append(group_dict)

        # Compute dimension scores (normalize raw_sum)
        for dim_data in dimensions.values():
            groups = dim_data["groups"]
            if groups:
                factor_scores = [
                    g["valuation"]["factor_score"]
                    for g in groups
                    if g["valuation"] and g["valuation"]["factor_score"] is not None
                ]
                dim_data["dimension_score"] = (
                    sum(factor_scores) / len(factor_scores) if factor_scores else 0.0
                )
                dim_data["group_count"] = len(groups)
            else:
                dim_data["dimension_score"] = 0.0
                dim_data["group_count"] = 0

        # Compute per-stage dollar_mid sums for evidence basis display
        plan_dollars = 0.0
        investment_dollars = 0.0
        capture_dollars = 0.0
        for gm in group_models:
            vm = val_by_group.get(gm.id)
            if vm and vm.dollar_mid:
                if vm.evidence_type == "plan":
                    plan_dollars += vm.dollar_mid
                elif vm.evidence_type == "investment":
                    investment_dollars += vm.dollar_mid
                elif vm.evidence_type == "capture":
                    capture_dollars += vm.dollar_mid

        return {
            "ticker": company.ticker,
            "requested_ticker": requested_ticker,
            "is_alias": is_alias,
            "canonical_ticker": company.ticker if is_alias else None,
            "child_tickers": child_tickers,
            "total_passages": total_passages,
            "total_groups": total_groups,
            "type_counts": type_counts,
            "dimensions": dimensions,
            "plan_dollars": round(plan_dollars, 2),
            "investment_dollars": round(investment_dollars, 2),
            "capture_dollars": round(capture_dollars, 2),
        }

    # ── Investment Projects ──────────────────────────────────────────────

    async def save_investment_projects(
        self,
        projects: list[SynthesizedProject],
    ) -> list[SynthesizedProject]:
        """Save synthesized projects to the database.

        Deletes existing projects for the same company before inserting.
        """
        if not projects:
            return []

        company_id = projects[0].company_id

        # Delete existing projects for this company
        await self.session.execute(
            delete(InvestmentProjectModel).where(
                InvestmentProjectModel.company_id == company_id
            )
        )

        saved = []
        for p in projects:
            m = InvestmentProjectModel(
                company_id=p.company_id,
                pipeline_run_id=p.pipeline_run_id,
                short_title=p.short_title,
                description=p.description,
                target_dimension=p.target_dimension,
                target_subcategory=p.target_subcategory,
                target_detail=p.target_detail,
                status=p.status,
                dollar_total=p.dollar_total,
                dollar_low=p.dollar_low,
                dollar_high=p.dollar_high,
                confidence=p.confidence,
                evidence_count=p.evidence_count,
                date_start=p.date_start,
                date_end=p.date_end,
                technology_area=p.technology_area,
                deployment_scope=p.deployment_scope,
                evidence_group_ids=p.evidence_group_ids,
                valuation_ids=p.valuation_ids,
            )
            self.session.add(m)
            await self.session.flush()
            p.id = m.id
            saved.append(p)
        return saved

    async def get_investment_projects_for_company(
        self,
        company_id: int,
    ) -> list[SynthesizedProject]:
        """Get all synthesized projects for a company."""
        stmt = (
            select(InvestmentProjectModel)
            .where(InvestmentProjectModel.company_id == company_id)
            .order_by(InvestmentProjectModel.id)
        )
        result = await self.session.execute(stmt)
        models = result.scalars().all()
        return [
            SynthesizedProject(
                id=m.id,
                company_id=m.company_id,
                pipeline_run_id=m.pipeline_run_id,
                short_title=m.short_title,
                description=m.description,
                target_dimension=m.target_dimension,
                target_subcategory=m.target_subcategory,
                target_detail=m.target_detail or "",
                status=m.status or "planned",
                dollar_total=m.dollar_total,
                dollar_low=m.dollar_low,
                dollar_high=m.dollar_high,
                confidence=m.confidence or 0.0,
                evidence_count=m.evidence_count or 0,
                date_start=m.date_start,
                date_end=m.date_end,
                technology_area=m.technology_area or "",
                deployment_scope=m.deployment_scope or "",
                evidence_group_ids=m.evidence_group_ids or [],
                valuation_ids=m.valuation_ids or [],
            )
            for m in models
        ]
