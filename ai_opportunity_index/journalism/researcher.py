"""Researcher agent: discovers and collects raw sources.

Executes research tasks using existing data collectors, extracts relevant
passages, tags metadata, and delivers raw material with full provenance.
"""

from __future__ import annotations

import logging
from datetime import date, datetime

from ai_opportunity_index.domains import CollectedItem, SourceAuthority

from .models import ResearchResult, ResearchTask

logger = logging.getLogger(__name__)


class Researcher:
    """Discovers and collects raw sources for research tasks."""

    def __init__(self, gnews_api_key: str | None = None):
        self.gnews_api_key = gnews_api_key

    def execute_task(self, task: ResearchTask) -> ResearchResult:
        """Execute a research task using existing collection infrastructure.

        Given a research task, use the existing data collection functions
        (news_signals, web_enrichment, etc.) and return structured results.
        """
        result = ResearchResult(task=task)

        try:
            items = self._collect_from_sources(
                ticker=task.ticker,
                company_name=task.company_name,
                dimensions=task.dimensions,
            )
            result.collected_items = items
            result.sources_succeeded = sum(1 for item in items if item.content)
        except Exception as e:
            logger.exception("Research task failed for %s", task.ticker)
            result.errors.append(str(e))

        result.sources_attempted = result.sources_succeeded + len(result.errors)
        return result

    def _collect_from_sources(
        self,
        ticker: str | None,
        company_name: str | None,
        dimensions: list[str],
    ) -> list[CollectedItem]:
        """Orchestrate collection across multiple sources.

        Uses existing data collection functions:
        - news_signals.search_company_news for news articles
        - web_enrichment.fetch_web_enrichment for web page signals
        """
        if not ticker or not company_name:
            logger.warning("Cannot collect without ticker and company_name")
            return []

        items: list[CollectedItem] = []

        # 1. News collection
        items.extend(self._collect_news(ticker, company_name))

        # 2. Web enrichment (if company has known URLs, handled downstream)
        items.extend(self._collect_web_signals(ticker, company_name))

        return items

    def _collect_news(
        self, ticker: str, company_name: str
    ) -> list[CollectedItem]:
        """Collect news articles about a company."""
        try:
            from ai_opportunity_index.data.news_signals import search_company_news

            articles = search_company_news(
                company_name=company_name,
                ticker=ticker,
                days_back=90,
                api_key=self.gnews_api_key,
            )
            logger.info(
                "Collected %d news articles for %s", len(articles), ticker
            )
            return articles
        except Exception as e:
            logger.warning("News collection failed for %s: %s", ticker, e)
            return []

    def _collect_web_signals(
        self, ticker: str, company_name: str
    ) -> list[CollectedItem]:
        """Collect web enrichment signals from company pages.

        Looks up company URLs from the DB and scrapes careers, IR,
        and blog pages for AI-related signals.
        """
        try:
            from ai_opportunity_index.storage.db import get_session
            from ai_opportunity_index.storage.models import CompanyModel
            from sqlalchemy import select

            session = get_session()
            try:
                company = session.execute(
                    select(CompanyModel).where(CompanyModel.ticker == ticker)
                ).scalar_one_or_none()

                if not company:
                    return []

                careers_url = company.careers_url
                ir_url = company.ir_url
                blog_url = company.blog_url

                if not any([careers_url, ir_url, blog_url]):
                    return []

                from ai_opportunity_index.data.web_enrichment import (
                    fetch_web_enrichment,
                )

                web_data = fetch_web_enrichment(
                    ticker=ticker,
                    company_name=company_name,
                    careers_url=careers_url,
                    ir_url=ir_url,
                    blog_url=blog_url,
                )

                # Convert web enrichment results to CollectedItems
                items: list[CollectedItem] = []
                today = date.today()

                for section_key, source_type, url in [
                    ("careers", "web_careers", careers_url),
                    ("investor_relations", "web_ir", ir_url),
                    ("blog", "web_blog", blog_url),
                ]:
                    section = web_data.get(section_key)
                    if not section:
                        continue

                    summary = section.get("page_summary", "")
                    if not summary:
                        continue

                    items.append(CollectedItem(
                        item_id=f"{ticker}:{source_type}:{today.isoformat()}",
                        title=f"{company_name} - {section_key.replace('_', ' ').title()}",
                        content=summary,
                        author=company_name,
                        publisher=company_name,
                        url=url,
                        source_date=today,
                        access_date=today,
                        authority=SourceAuthority.FIRST_PARTY_PUBLIC,
                    ))

                return items
            finally:
                session.close()
        except Exception as e:
            logger.warning("Web enrichment failed for %s: %s", ticker, e)
            return []
