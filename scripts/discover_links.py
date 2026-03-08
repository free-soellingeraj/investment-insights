#!/usr/bin/env python3
"""Discover and populate company URLs (GitHub, careers, investor relations, blog).

Uses Gemini Flash with Google Search grounding to find official company URLs
in a single LLM call per company. GitHub discovery also uses the GitHub API.

Usage:
    python scripts/discover_links.py [--limit N] [--tickers TICK1 TICK2]
    python scripts/discover_links.py --sources website --limit 50
    python scripts/discover_links.py --sources github --tickers AAPL MSFT
"""

import argparse
import asyncio
import json
import logging
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
import re

import requests
from google import genai
from google.genai import types
from pydantic import BaseModel

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ai_opportunity_index.cache import cache_is_fresh, stamp_cache
from ai_opportunity_index.config import (
    GITHUB_RATE_LIMIT_SECONDS,
    GITHUB_TOKEN,
    LINK_DISCOVERY_CONCURRENCY,
    LINK_DISCOVERY_MODEL,
    RAW_DIR,
)
from ai_opportunity_index.domains import (
    AIOpportunityEvidence,
    CaptureStage,
    EvidenceSourceType,
    FinancialObservation,
    FinancialMetric,
    FinancialUnits,
    PipelineRun,
    PipelineSubtask,
    PipelineTask,
    RunStatus,
    RunType,
    SignalStrength,
    TargetDimension,
)
from ai_opportunity_index.storage.db import (
    complete_pipeline_run,
    create_company_venture,
    create_pipeline_run,
    get_latest_financials,
    get_or_create_company_by_slug,
    get_session,
    init_db,
    save_evidence,
    save_financial_observations_batch,
    _slugify,
)
from ai_opportunity_index.storage.models import CompanyModel, CompanyVentureModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

GITHUB_API = "https://api.github.com"
DISCOVERED_LINKS_DIR = RAW_DIR / "discovered_links"

# ── Pydantic output model for LLM ────────────────────────────────────────


class DiscoveredSubsidiary(BaseModel):
    name: str
    ticker: str | None = None
    ownership_pct: float | None = None
    relationship_type: str = "subsidiary"
    website_url: str | None = None
    sector: str | None = None
    industry: str | None = None
    country: str | None = None
    exchange: str | None = None


class DiscoveredFinancial(BaseModel):
    metric: str  # 'market_cap', 'revenue', 'net_income', 'employees'
    value: float
    value_units: str  # 'usd', 'count'
    fiscal_period: str | None = None  # 'FY2024', 'FY2025', etc.


class DiscoveredLinks(BaseModel):
    github_url: str | None = None
    careers_url: str | None = None
    ir_url: str | None = None
    blog_url: str | None = None
    sector: str | None = None
    industry: str | None = None
    exchange: str | None = None
    country: str | None = None
    financials: list[DiscoveredFinancial] = []
    subsidiaries: list[DiscoveredSubsidiary] = []


# ── GitHub discovery (unchanged) ─────────────────────────────────────────


def _github_headers() -> dict:
    h = {"Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        h["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return h


def discover_github_url(company_name: str) -> str | None:
    """Find the GitHub org URL for a company."""
    clean_name = company_name.split(",")[0].split(" Inc")[0].split(" Corp")[0].strip()
    try:
        resp = requests.get(
            f"{GITHUB_API}/search/users",
            params={"q": f"{clean_name} type:org", "per_page": 3},
            headers=_github_headers(),
            timeout=15,
        )
        if resp.status_code != 200:
            return None
        items = resp.json().get("items", [])
        if items:
            return items[0]["html_url"]
    except Exception as e:
        logger.debug("GitHub URL discovery failed for %s: %s", company_name, e)
    return None


# ── LLM link search with Google Search grounding ────────────────────────


def _get_genai_client():
    """Lazy-init google-genai client for Vertex AI."""
    if not hasattr(_get_genai_client, "_client"):
        _get_genai_client._client = genai.Client(
            vertexai=True, project="prgrn-ai", location="us-central1",
        )
    return _get_genai_client._client


def _parse_urls_from_text(text: str) -> DiscoveredLinks:
    """Parse JSON or labeled URLs from LLM response text."""
    # Try to extract JSON block first
    # Try to find the outermost JSON object (may contain nested arrays)
    # Find first { and last } to capture full JSON
    brace_start = text.find("{")
    brace_end = text.rfind("}")
    if brace_start >= 0 and brace_end > brace_start:
        try:
            data = json.loads(text[brace_start:brace_end + 1])
            subs = []
            for s in data.get("subsidiaries", []):
                if isinstance(s, dict) and s.get("name"):
                    subs.append(DiscoveredSubsidiary(
                        name=s["name"],
                        ticker=s.get("ticker") or None,
                        ownership_pct=s.get("ownership_pct"),
                        relationship_type=s.get("relationship_type", "subsidiary"),
                        website_url=s.get("website_url") or None,
                        sector=s.get("sector") or None,
                        industry=s.get("industry") or None,
                        country=s.get("country") or None,
                        exchange=s.get("exchange") or None,
                    ))
            fins = []
            for f in data.get("financials", []):
                if isinstance(f, dict) and f.get("metric") and f.get("value") is not None:
                    try:
                        fins.append(DiscoveredFinancial(
                            metric=f["metric"],
                            value=float(f["value"]),
                            value_units=f.get("value_units", "usd"),
                            fiscal_period=f.get("fiscal_period"),
                        ))
                    except (ValueError, TypeError):
                        pass
            return DiscoveredLinks(
                github_url=data.get("github_url") or None,
                careers_url=data.get("careers_url") or None,
                ir_url=data.get("ir_url") or None,
                blog_url=data.get("blog_url") or None,
                sector=data.get("sector") or None,
                industry=data.get("industry") or None,
                exchange=data.get("exchange") or None,
                country=data.get("country") or None,
                financials=fins,
                subsidiaries=subs,
            )
        except json.JSONDecodeError:
            pass

    # Fallback: extract labeled URLs from text
    result = {}
    patterns = {
        "github_url": r"github[_ ]?url[\"']?\s*[:=]\s*[\"']?(https?://[^\s\"',}]+)",
        "careers_url": r"careers[_ ]?url[\"']?\s*[:=]\s*[\"']?(https?://[^\s\"',}]+)",
        "ir_url": r"ir[_ ]?url[\"']?\s*[:=]\s*[\"']?(https?://[^\s\"',}]+)",
        "blog_url": r"blog[_ ]?url[\"']?\s*[:=]\s*[\"']?(https?://[^\s\"',}]+)",
    }
    for key, pattern in patterns.items():
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            result[key] = m.group(1)

    return DiscoveredLinks(**result)


async def search_company_links(
    company_name: str,
    parent_context: str | None = None,
) -> DiscoveredLinks:
    """Use Gemini with Google Search grounding to find company URLs.

    Single LLM call with Google Search enabled — no need to scrape homepages.
    """
    client = _get_genai_client()
    search_tool = types.Tool(google_search=types.GoogleSearch())
    config = types.GenerateContentConfig(tools=[search_tool])

    context_line = ""
    if parent_context:
        context_line = (
            f"\nIMPORTANT CONTEXT: {parent_context}\n"
            f"Use this context to identify the correct entity — do NOT confuse it with unrelated companies that share a similar name.\n"
            f"If you cannot find information specific to this entity, return null for those fields rather than returning data for a different company.\n\n"
        )

    prompt = (
        f"Use Google Search to find the following information for the company \"{company_name}\":\n"
        f"{context_line}\n"
        f"**URLs:**\n"
        f"1. github_url — GitHub organization page (e.g. https://github.com/apple)\n"
        f"2. careers_url — Official careers or jobs page\n"
        f"3. ir_url — Investor relations page\n"
        f"4. blog_url — Official blog or newsroom page\n\n"
        f"**Company demographics:**\n"
        f"5. sector — GICS sector (e.g. \"Information Technology\", \"Health Care\", \"Financials\")\n"
        f"6. industry — GICS industry (e.g. \"Systems Software\", \"Semiconductors\", \"Application Software\")\n"
        f"7. exchange — Stock exchange the ticker is listed on (e.g. \"NASDAQ\", \"NYSE\"), or null if private\n"
        f"8. country — HQ country (e.g. \"US\", \"UK\", \"JP\")\n\n"
        f"**Financials** (most recent available data):\n"
        f"9. financials — Array of financial metrics. Include any of: market_cap (USD), revenue (annual, USD), net_income (annual, USD), employees (count).\n"
        f"   For each: {{\"metric\": \"...\", \"value\": 123.0, \"value_units\": \"usd\" or \"count\", \"fiscal_period\": \"FY2024\"}}\n\n"
        f"**Subsidiaries:**\n"
        f"10. subsidiaries — List of subsidiaries, joint ventures, or owned companies that do AI/tech work.\n"
        f"   For each, include as much detail as possible: {{\"name\": \"...\", \"ticker\": \"...\" or null, \"ownership_pct\": 0.0-1.0, "
        f"\"relationship_type\": \"subsidiary|joint_venture|strategic_investment\", "
        f"\"website_url\": \"https://...\", \"sector\": \"...\", \"industry\": \"...\", \"country\": \"...\", \"exchange\": \"...\" or null}}\n\n"
        f"Respond with ONLY a JSON object like:\n"
        f'{{"github_url": "...", "careers_url": "...", "ir_url": "...", "blog_url": "...", '
        f'"sector": "Information Technology", "industry": "Systems Software", "exchange": "NASDAQ", "country": "US", '
        f'"financials": [{{"metric": "market_cap", "value": 3000000000000, "value_units": "usd", "fiscal_period": null}}, '
        f'{{"metric": "revenue", "value": 383000000000, "value_units": "usd", "fiscal_period": "FY2024"}}, '
        f'{{"metric": "employees", "value": 161000, "value_units": "count", "fiscal_period": "FY2024"}}], '
        f'"subsidiaries": [{{"name": "Example AI Labs", "ticker": null, "ownership_pct": 1.0, "relationship_type": "subsidiary", '
        f'"website_url": "https://example.com", "sector": "Information Technology", "industry": "Application Software", "country": "US", "exchange": null}}]}}\n'
        f"Use null for any field you cannot find. Use empty arrays for financials/subsidiaries if none are found."
    )

    try:
        response = await asyncio.to_thread(
            client.models.generate_content,
            model=LINK_DISCOVERY_MODEL,
            contents=prompt,
            config=config,
        )
        return _parse_urls_from_text(response.text)
    except Exception as e:
        logger.warning("LLM search failed for %s: %s", company_name, e)
        return DiscoveredLinks()




# ── Per-company discovery orchestrator ───────────────────────────────────


async def discover_company_links(
    ticker: str, company_name: str, force: bool = False,
    company_id: int | None = None,
) -> dict | None:
    """Discover website links for a company via Gemini + Google Search.

    Single LLM call with search grounding — no homepage scraping needed.
    Returns a dict with discovered URLs, or None if nothing found.
    Also discovers subsidiaries and creates Company + venture records for them.
    """
    # Check cache first
    cache_path = DISCOVERED_LINKS_DIR / f"{ticker.upper()}.json"
    if cache_is_fresh(cache_path, "discover_links", force=force):
        logger.debug("Cache hit for %s, skipping", ticker)
        return None

    # Build parent context for subsidiaries so the LLM can disambiguate
    parent_context = None
    if company_id:
        try:
            session = get_session()
            venture = session.query(CompanyVentureModel).filter_by(
                subsidiary_id=company_id
            ).first()
            if venture:
                parent = session.query(CompanyModel).filter_by(id=venture.parent_id).first()
                if parent:
                    parent_label = parent.company_name or parent.ticker or parent.slug
                    rel = venture.relationship_type or "subsidiary"
                    pct = f" ({venture.ownership_pct * 100:.0f}% owned)" if venture.ownership_pct else ""
                    parent_ticker = f" (ticker: {parent.ticker})" if parent.ticker else ""
                    parent_sector = f", sector: {parent.sector}" if parent.sector else ""
                    parent_context = (
                        f"\"{company_name}\" is a {rel} of {parent_label}{parent_ticker}{parent_sector}{pct}."
                    )
                    logger.info("Using parent context for %s: %s", ticker, parent_context)
            session.close()
        except Exception as e:
            logger.debug("Failed to look up parent context for %s: %s", ticker, e)

    discovered = await search_company_links(company_name, parent_context=parent_context)

    result = {
        "ticker": ticker,
        "github_url": discovered.github_url,
        "careers_url": discovered.careers_url,
        "ir_url": discovered.ir_url,
        "blog_url": discovered.blog_url,
        "sector": discovered.sector,
        "industry": discovered.industry,
        "exchange": discovered.exchange,
        "country": discovered.country,
        "financials": [f.model_dump() for f in discovered.financials],
        "subsidiaries": [s.model_dump() for s in discovered.subsidiaries],
        "discovered_at": datetime.utcnow().isoformat(),
    }

    # Save company demographics (sector, industry, exchange, country)
    if company_id:
        try:
            session = get_session()
            co = session.query(CompanyModel).filter_by(id=company_id).first()
            if co:
                changed = False
                for attr in ("sector", "industry", "exchange", "country"):
                    val = getattr(discovered, attr, None)
                    if val and (not getattr(co, attr, None) or force):
                        setattr(co, attr, val)
                        changed = True
                if changed:
                    session.commit()
                else:
                    session.close()
            else:
                session.close()
        except Exception as e:
            logger.warning("Failed to save demographics for %s: %s", ticker, e)

    # Save financial observations (only if no existing data for that metric)
    if discovered.financials and company_id:
        existing_financials = get_latest_financials(company_id)
        new_obs = []
        for fin in discovered.financials:
            if fin.metric not in existing_financials or force:
                new_obs.append(FinancialObservation(
                    company_id=company_id,
                    metric=fin.metric,
                    value=fin.value,
                    value_units=fin.value_units,
                    source_datetime=datetime.utcnow(),
                    source_name="llm_discovery",
                    fiscal_period=fin.fiscal_period,
                ))
        if new_obs:
            try:
                save_financial_observations_batch(new_obs)
                logger.info("Saved %d financial observations for %s", len(new_obs), ticker)
            except Exception as e:
                logger.warning("Failed to save financials for %s: %s", ticker, e)

    # Create subsidiary Company records and venture links (metadata-only)
    if discovered.subsidiaries and company_id:
        for sub in discovered.subsidiaries:
            try:
                sub_ticker = sub.ticker.upper() if sub.ticker else None
                sub_slug = sub_ticker if sub_ticker else _slugify(sub.name)
                sub_company = get_or_create_company_by_slug(
                    slug=sub_slug,
                    company_name=sub.name,
                    ticker=sub_ticker,
                    sector=sub.sector,
                    industry=sub.industry,
                    country=sub.country,
                    exchange=sub.exchange,
                )
                create_company_venture(
                    parent_id=company_id,
                    subsidiary_id=sub_company.id,
                    ownership_pct=sub.ownership_pct,
                    relationship_type=sub.relationship_type,
                )

                # Create evidence records on both parent and subsidiary
                pct_str = f" ({sub.ownership_pct * 100:.0f}% owned)" if sub.ownership_pct else ""
                excerpt = (
                    f"Discovered {sub.relationship_type}: {sub.name}{pct_str}. "
                    f"Identified via LLM-grounded search during link discovery for {company_name} ({ticker})."
                )
                now = datetime.utcnow()
                today = now.date()
                evidence_payload = {
                    "subsidiary_name": sub.name,
                    "subsidiary_ticker": sub_ticker,
                    "subsidiary_slug": sub_slug,
                    "parent_ticker": ticker,
                    "parent_company_id": company_id,
                    "ownership_pct": sub.ownership_pct,
                    "relationship_type": sub.relationship_type,
                }
                for cid in (company_id, sub_company.id):
                    try:
                        save_evidence(AIOpportunityEvidence(
                            company_id=cid,
                            evidence_type=EvidenceSourceType.SUBSIDIARY_DISCOVERY,
                            evidence_subtype=sub.relationship_type,
                            source_name="llm_discovery",
                            source_date=today,
                            signal_strength=SignalStrength.MEDIUM,
                            target_dimension=TargetDimension.GENERAL,
                            capture_stage=CaptureStage.INVESTED,
                            source_excerpt=excerpt,
                            payload=evidence_payload,
                            observed_at=now,
                            valid_from=today,
                        ))
                    except Exception as e:
                        logger.debug("Failed to save subsidiary evidence for company %d: %s", cid, e)

                logger.info(
                    "Created venture link: %s -> %s (%s, %.0f%%)",
                    ticker, sub.name, sub.relationship_type,
                    (sub.ownership_pct or 0) * 100,
                )
            except Exception as e:
                logger.warning("Failed to create subsidiary %s for %s: %s", sub.name, ticker, e)

    # Only cache/return if we found at least one URL, demographic, financial, or subsidiary
    has_urls = any(result.get(k) for k in ("github_url", "careers_url", "ir_url", "blog_url"))
    has_demographics = any(result.get(k) for k in ("sector", "industry", "exchange", "country"))
    has_financials = bool(discovered.financials)
    has_subs = bool(discovered.subsidiaries)
    if not has_urls and not has_demographics and not has_financials and not has_subs:
        return None

    DISCOVERED_LINKS_DIR.mkdir(parents=True, exist_ok=True)
    stamp_cache(result, "discover_links")
    cache_path.write_text(json.dumps(result, indent=2))

    return result


# ── Main ─────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Discover company URLs")
    parser.add_argument("--tickers", nargs="*", help="Specific tickers")
    parser.add_argument("--limit", type=int, default=None, help="Limit companies")
    parser.add_argument("--force", action="store_true", help="Overwrite existing URLs")
    parser.add_argument(
        "--sources",
        nargs="*",
        default=["all"],
        choices=["website", "github", "all"],
        help="Which discovery methods to use (default: all)",
    )
    args = parser.parse_args()

    # Normalize sources
    sources = set(args.sources)
    if "all" in sources:
        sources = {"website", "github"}

    init_db()
    session = get_session()

    query = session.query(CompanyModel).filter(CompanyModel.is_active.is_(True))
    if args.tickers:
        from sqlalchemy import or_
        upper_tickers = [t.upper() for t in args.tickers]
        query = query.filter(
            or_(
                CompanyModel.ticker.in_(upper_tickers),
                CompanyModel.slug.in_(upper_tickers),
            )
        )
    if args.limit:
        query = query.limit(args.limit)
    companies = query.all()
    ticker_list = [c.ticker or c.slug for c in companies]
    logger.info("Discovering links for %d companies", len(companies))

    # Create a collect stage pipeline run
    run_id = str(uuid.uuid4())
    run_type = RunType.PARTIAL if args.tickers else RunType.FULL
    create_pipeline_run(PipelineRun(
        run_id=run_id,
        task=PipelineTask.COLLECT,
        subtask=PipelineSubtask.LINKS,
        run_type=run_type,
        status=RunStatus.RUNNING,
        tickers_requested=ticker_list,
        parameters={
            "sources": list(sources),
            "force": args.force,
            "source": "discover_links",
        },
    ))

    updated = 0

    try:
        _run_discovery(companies, sources, args, session)
    except Exception as exc:
        complete_pipeline_run(
            run_id=run_id,
            status=RunStatus.FAILED,
            error_message=str(exc),
        )
        session.close()
        raise

    complete_pipeline_run(
        run_id=run_id,
        status=RunStatus.COMPLETED,
        tickers_succeeded=len(companies),
    )
    session.close()
    logger.info("Link discovery complete: %d total companies processed", len(companies))


def _run_discovery(companies, sources, args, session):
    """Run the actual discovery logic."""
    updated = 0

    # ── Website-based discovery (LLM) with concurrency ───────────────
    if "website" in sources:
        logger.info("=== Website link discovery (LLM-based) ===")

        async def _process_batch():
            sem = asyncio.Semaphore(LINK_DISCOVERY_CONCURRENCY)
            results = {}
            completed = 0

            async def _process_one(company):
                nonlocal completed
                async with sem:
                    ident = company.ticker or company.slug
                    name = company.company_name or ident
                    try:
                        result = await discover_company_links(
                            ident, name, force=args.force,
                            company_id=company.id,
                        )
                        if result:
                            results[ident] = result
                    except Exception as e:
                        logger.warning(
                            "Website discovery failed for %s: %s", ident, e
                        )
                    finally:
                        completed += 1
                        if completed % 50 == 0:
                            logger.info(
                                "Website discovery progress: %d/%d (%d found)",
                                completed, len(companies), len(results),
                            )

            await asyncio.gather(*[_process_one(c) for c in companies])
            return results

        website_results = asyncio.run(_process_batch())

        # Batch update DB from results
        for company in companies:
            ident = company.ticker or company.slug
            result = website_results.get(ident)
            if not result:
                continue

            changed = False
            for attr in ("github_url", "careers_url", "ir_url", "blog_url",
                         "sector", "industry", "exchange", "country"):
                val = result.get(attr)
                if val and (not getattr(company, attr, None) or args.force):
                    setattr(company, attr, val)
                    changed = True
            if changed:
                updated += 1

        session.commit()
        logger.info(
            "Website discovery complete: %d companies updated, %d cached",
            updated, len(website_results),
        )

    # ── GitHub discovery (serial, rate-limited) ──────────────────────
    if "github" in sources:
        logger.info("=== GitHub URL discovery ===")
        gh_updated = 0
        for i, company in enumerate(companies):
            if company.github_url and not args.force:
                continue

            name = company.company_name or company.ticker or company.slug
            url = discover_github_url(name)
            if url:
                company.github_url = url
                gh_updated += 1
            time.sleep(GITHUB_RATE_LIMIT_SECONDS)

            if (i + 1) % 50 == 0:
                session.commit()
                logger.info(
                    "GitHub progress: %d/%d (updated: %d)",
                    i + 1, len(companies), gh_updated,
                )

        session.commit()
        updated += gh_updated
        logger.info("GitHub discovery complete: %d companies updated", gh_updated)


if __name__ == "__main__":
    main()
