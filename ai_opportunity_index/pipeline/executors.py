"""Stage executor functions for the pipeline.

Each exec_* function runs a single pipeline stage for a single company.
Moved from scripts/run_pipeline.py.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime

from ai_opportunity_index.pipeline.dag import StageResult
from ai_opportunity_index.storage.db import get_session
from ai_opportunity_index.storage.models import CompanyModel

logger = logging.getLogger(__name__)


async def exec_discover_links(
    company: CompanyModel, force: bool,
) -> StageResult:
    ticker = company.ticker or company.slug
    name = company.company_name or ticker
    logger.info("[%s] discover_links starting", ticker)
    try:
        import json
        from scripts.discover_links import discover_company_links, DISCOVERED_LINKS_DIR

        result = await discover_company_links(ticker, name, force=force, company_id=company.id)

        # If cached (result is None), read from cache file for DB update
        if result is None:
            cache_path = DISCOVERED_LINKS_DIR / f"{ticker.upper()}.json"
            if cache_path.exists():
                try:
                    result = json.loads(cache_path.read_text())
                except Exception:
                    pass

        # Update company URL columns in DB from discovered links
        if result:
            session = get_session()
            try:
                co = session.query(CompanyModel).filter_by(id=company.id).first()
                if co:
                    changed = False
                    for attr in ("github_url", "careers_url", "ir_url", "blog_url",
                                 "sector", "industry", "exchange", "country"):
                        val = result.get(attr)
                        if val and (not getattr(co, attr, None) or force):
                            setattr(co, attr, val)
                            changed = True
                    if changed:
                        session.commit()
            except Exception:
                session.rollback()
            finally:
                session.close()

        logger.info("[%s] discover_links done", ticker)
        return StageResult("discover_links", ticker, success=True)
    except Exception as e:
        logger.warning("[%s] discover_links failed: %s", ticker, e)
        return StageResult("discover_links", ticker, success=False, error=str(e))


async def exec_collect_news(
    company: CompanyModel, force: bool,
) -> StageResult:
    ticker = company.ticker or company.slug
    logger.info("[%s] collect_news starting", ticker)
    try:
        from scripts.collect_evidence import collect_news

        await asyncio.to_thread(collect_news, [company], force=force)
        logger.info("[%s] collect_news done", ticker)
        return StageResult("collect_news", ticker, success=True)
    except Exception as e:
        logger.warning("[%s] collect_news failed: %s", ticker, e)
        return StageResult("collect_news", ticker, success=False, error=str(e))


async def exec_collect_github(
    company: CompanyModel, force: bool,
) -> StageResult:
    ticker = company.ticker or company.slug
    logger.info("[%s] collect_github starting", ticker)
    try:
        from scripts.collect_evidence import collect_github

        await asyncio.to_thread(collect_github, [company], force=force)
        logger.info("[%s] collect_github done", ticker)
        return StageResult("collect_github", ticker, success=True)
    except Exception as e:
        logger.warning("[%s] collect_github failed: %s", ticker, e)
        return StageResult("collect_github", ticker, success=False, error=str(e))


async def exec_collect_analysts(
    company: CompanyModel, force: bool,
) -> StageResult:
    ticker = company.ticker or company.slug
    logger.info("[%s] collect_analysts starting", ticker)
    try:
        from scripts.collect_evidence import collect_analysts

        await asyncio.to_thread(collect_analysts, [company], force=force)
        logger.info("[%s] collect_analysts done", ticker)
        return StageResult("collect_analysts", ticker, success=True)
    except Exception as e:
        logger.warning("[%s] collect_analysts failed: %s", ticker, e)
        return StageResult("collect_analysts", ticker, success=False, error=str(e))


async def exec_collect_web_enrichment(
    company: CompanyModel, force: bool,
) -> StageResult:
    ticker = company.ticker or company.slug
    logger.info("[%s] collect_web_enrichment starting", ticker)
    try:
        from scripts.collect_evidence import collect_web_enrichment

        await asyncio.to_thread(collect_web_enrichment, [company], force=force)
        logger.info("[%s] collect_web_enrichment done", ticker)
        return StageResult("collect_web_enrichment", ticker, success=True)
    except Exception as e:
        logger.warning("[%s] collect_web_enrichment failed: %s", ticker, e)
        return StageResult("collect_web_enrichment", ticker, success=False, error=str(e))


async def exec_extract_filings(
    company: CompanyModel,
    force: bool,
    llm_semaphore: asyncio.Semaphore | None = None,
) -> StageResult:
    ticker = company.ticker or company.slug
    logger.info("[%s] extract_filings starting", ticker)
    try:
        from ai_opportunity_index.data.filing_extraction import extract_and_cache_filings

        name = company.company_name or ticker
        await extract_and_cache_filings(
            ticker, company_name=name,
            sector=company.sector or "",
            force=force,
            semaphore=llm_semaphore,
        )
        logger.info("[%s] extract_filings done", ticker)
        return StageResult("extract_filings", ticker, success=True)
    except Exception as e:
        logger.warning("[%s] extract_filings failed: %s", ticker, e)
        return StageResult("extract_filings", ticker, success=False, error=str(e))


async def exec_extract_news(
    company: CompanyModel,
    force: bool,
    llm_semaphore: asyncio.Semaphore | None = None,
) -> StageResult:
    ticker = company.ticker or company.slug
    logger.info("[%s] extract_news starting", ticker)
    try:
        from ai_opportunity_index.data.news_extraction import extract_and_cache_news

        name = company.company_name or ticker
        await extract_and_cache_news(
            ticker, company_name=name,
            force=force,
            semaphore=llm_semaphore,
        )
        logger.info("[%s] extract_news done", ticker)
        return StageResult("extract_news", ticker, success=True)
    except Exception as e:
        logger.warning("[%s] extract_news failed: %s", ticker, e)
        return StageResult("extract_news", ticker, success=False, error=str(e))


async def exec_extract_unified(
    company: CompanyModel,
    force: bool,
    llm_semaphore: asyncio.Semaphore | None = None,
) -> StageResult:
    """Run unified extraction on all collected source items."""
    ticker = company.ticker or company.slug
    logger.info("[%s] extract_unified starting", ticker)
    try:
        from ai_opportunity_index.data.unified_extraction import extract_for_company

        name = company.company_name or ticker
        counts = await extract_for_company(
            ticker, company_name=name,
            force=force,
            semaphore=llm_semaphore,
        )
        total = sum(counts.values())
        if total:
            logger.info("[%s] extract_unified done: %s", ticker, counts)
        else:
            logger.info("[%s] extract_unified done: no items to extract", ticker)
        return StageResult("extract_unified", ticker, success=True)
    except Exception as e:
        logger.warning("[%s] extract_unified failed: %s", ticker, e)
        return StageResult("extract_unified", ticker, success=False, error=str(e))


async def exec_value_evidence(
    company: CompanyModel,
    force: bool,
    llm_semaphore: asyncio.Semaphore | None = None,
) -> StageResult:
    ticker = company.ticker or company.slug
    logger.info("[%s] value_evidence starting", ticker)
    try:
        from ai_opportunity_index.scoring.evidence_valuation import value_evidence_for_company
        from ai_opportunity_index.storage.db import get_latest_financials

        # Skip only if ALL evidence groups have final valuations (not just "any exist")
        if not force:
            from ai_opportunity_index.storage.models import EvidenceGroupModel, ValuationModel
            from sqlalchemy import select, func as sa_func
            skip_session = get_session()
            try:
                total_groups = skip_session.execute(
                    select(sa_func.count()).select_from(EvidenceGroupModel)
                    .where(EvidenceGroupModel.company_id == company.id)
                ).scalar() or 0
                valued_groups = skip_session.execute(
                    select(sa_func.count(sa_func.distinct(ValuationModel.group_id)))
                    .join(EvidenceGroupModel)
                    .where(
                        EvidenceGroupModel.company_id == company.id,
                        ValuationModel.stage == "final",
                    )
                ).scalar() or 0
            finally:
                skip_session.close()

            if total_groups > 0 and valued_groups >= total_groups:
                logger.info(
                    "[%s] value_evidence skipped: all %d groups have final valuations",
                    ticker, total_groups,
                )
                return StageResult("value_evidence", ticker, success=True, skipped=True)
            elif valued_groups > 0:
                logger.info(
                    "[%s] value_evidence: %d/%d groups valued — re-running for remaining",
                    ticker, valued_groups, total_groups,
                )

        financials = get_latest_financials(company.id)
        revenue_obs = financials.get("revenue")
        revenue = revenue_obs.value if revenue_obs else 0.0

        result = await value_evidence_for_company(
            ticker=ticker,
            company_id=company.id,
            company_name=company.company_name or ticker,
            sector=company.sector or "",
            revenue=revenue,
            llm_semaphore=llm_semaphore,
        )
        if result:
            logger.info(
                "[%s] value_evidence done: %d groups, cost=%.4f rev=%.4f gen=%.4f",
                ticker, result["total_groups"],
                result["cost_score"], result["revenue_score"], result["general_score"],
            )
        else:
            logger.info("[%s] value_evidence done: no evidence to value", ticker)
        return StageResult("value_evidence", ticker, success=True)
    except Exception as e:
        logger.warning("[%s] value_evidence failed: %s", ticker, e)
        return StageResult("value_evidence", ticker, success=False, error=str(e))


async def exec_score(
    company: CompanyModel,
    pipeline_run_id: int,
    llm_semaphore: asyncio.Semaphore | None = None,
    force: bool = False,
) -> StageResult:
    ticker = company.ticker or company.slug
    logger.info("[%s] score starting", ticker)
    try:
        from scripts.score_companies import score_and_save_company_async

        # Only score if the company has evidence from the last year
        from ai_opportunity_index.storage.db import get_latest_score
        from ai_opportunity_index.storage.models import EvidenceModel
        from sqlalchemy import select, func as sa_func
        from datetime import timedelta

        evidence_session = get_session()
        try:
            one_year_ago = datetime.utcnow() - timedelta(days=365)
            recent_evidence_count = evidence_session.execute(
                select(sa_func.count())
                .where(EvidenceModel.company_id == company.id)
                .where(EvidenceModel.observed_at >= one_year_ago)
            ).scalar() or 0
        finally:
            evidence_session.close()

        if recent_evidence_count == 0:
            logger.info("[%s] score skipped: no evidence from the last year", ticker)
            return StageResult("score", ticker, success=True, skipped=True)

        # Skip if score already exists and not forced
        if not force:
            existing = get_latest_score(company.id)
            if existing:
                logger.info("[%s] score skipped: score already exists", ticker)
                return StageResult("score", ticker, success=True, skipped=True)

        session = get_session()
        try:
            now = datetime.utcnow()
            success = await score_and_save_company_async(
                company, session, pipeline_run_id, now,
                dollar_pipeline=True,
                llm_semaphore=llm_semaphore,
            )
            session.commit()
            logger.info("[%s] score done", ticker)
            return StageResult("score", ticker, success=success)
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    except Exception as e:
        logger.warning("[%s] score failed: %s", ticker, e)
        return StageResult("score", ticker, success=False, error=str(e))


STAGE_EXECUTORS = {
    "discover_links": exec_discover_links,
    "collect_news": exec_collect_news,
    "collect_github": exec_collect_github,
    "collect_analysts": exec_collect_analysts,
    "collect_web_enrichment": exec_collect_web_enrichment,
    "extract_filings": exec_extract_filings,
    "extract_news": exec_extract_news,
    "extract_unified": exec_extract_unified,
    "value_evidence": exec_value_evidence,
    "score": exec_score,
}
