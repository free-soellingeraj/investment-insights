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
from ai_opportunity_index.domains import PipelineRun, PipelineSubtask, PipelineTask
from ai_opportunity_index.storage.db import (
    complete_pipeline_run,
    create_pipeline_run,
    get_session,
    init_db,
)
from ai_opportunity_index.storage.models import CompanyModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

GITHUB_API = "https://api.github.com"
DISCOVERED_LINKS_DIR = RAW_DIR / "discovered_links"

# ── Pydantic output model for LLM ────────────────────────────────────────


class DiscoveredLinks(BaseModel):
    github_url: str | None = None
    careers_url: str | None = None
    ir_url: str | None = None
    blog_url: str | None = None


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
    json_match = re.search(r"\{[^{}]*\}", text, re.DOTALL)
    if json_match:
        try:
            data = json.loads(json_match.group())
            return DiscoveredLinks(
                github_url=data.get("github_url") or None,
                careers_url=data.get("careers_url") or None,
                ir_url=data.get("ir_url") or None,
                blog_url=data.get("blog_url") or None,
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
) -> DiscoveredLinks:
    """Use Gemini with Google Search grounding to find company URLs.

    Single LLM call with Google Search enabled — no need to scrape homepages.
    """
    client = _get_genai_client()
    search_tool = types.Tool(google_search=types.GoogleSearch())
    config = types.GenerateContentConfig(tools=[search_tool])

    prompt = (
        f"Use Google Search to find the following official URLs for the company \"{company_name}\":\n"
        f"1. github_url — GitHub organization page (e.g. https://github.com/apple)\n"
        f"2. careers_url — Official careers or jobs page\n"
        f"3. ir_url — Investor relations page\n"
        f"4. blog_url — Official blog or newsroom page\n\n"
        f"Respond with ONLY a JSON object like:\n"
        f'{{"github_url": "...", "careers_url": "...", "ir_url": "...", "blog_url": "..."}}\n'
        f"Use null for any URL you cannot find."
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
    ticker: str, company_name: str, force: bool = False
) -> dict | None:
    """Discover website links for a company via Gemini + Google Search.

    Single LLM call with search grounding — no homepage scraping needed.
    Returns a dict with discovered URLs, or None if nothing found.
    """
    # Check cache first
    cache_path = DISCOVERED_LINKS_DIR / f"{ticker.upper()}.json"
    if cache_is_fresh(cache_path, "discover_links", force=force):
        logger.debug("Cache hit for %s, skipping", ticker)
        return None

    discovered = await search_company_links(company_name)

    result = {
        "ticker": ticker,
        "github_url": discovered.github_url,
        "careers_url": discovered.careers_url,
        "ir_url": discovered.ir_url,
        "blog_url": discovered.blog_url,
        "discovered_at": datetime.utcnow().isoformat(),
    }

    # Only cache/return if we found at least one URL
    if not any(result.get(k) for k in ("github_url", "careers_url", "ir_url", "blog_url")):
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
        query = query.filter(CompanyModel.ticker.in_([t.upper() for t in args.tickers]))
    if args.limit:
        query = query.limit(args.limit)
    companies = query.all()
    ticker_list = [c.ticker for c in companies]
    logger.info("Discovering links for %d companies", len(companies))

    # Create a collect stage pipeline run
    run_id = str(uuid.uuid4())
    run_type = "partial" if args.tickers else "full"
    create_pipeline_run(PipelineRun(
        run_id=run_id,
        task=PipelineTask.COLLECT,
        subtask=PipelineSubtask.LINKS,
        run_type=run_type,
        status="running",
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
            status="failed",
            error_message=str(exc),
        )
        session.close()
        raise

    complete_pipeline_run(
        run_id=run_id,
        status="completed",
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
                    name = company.company_name or company.ticker
                    try:
                        result = await discover_company_links(
                            company.ticker, name, force=args.force
                        )
                        if result:
                            results[company.ticker] = result
                    except Exception as e:
                        logger.warning(
                            "Website discovery failed for %s: %s", company.ticker, e
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
            result = website_results.get(company.ticker)
            if not result:
                continue

            changed = False
            if result.get("github_url") and (not company.github_url or args.force):
                company.github_url = result["github_url"]
                changed = True
            if result.get("careers_url") and (not company.careers_url or args.force):
                company.careers_url = result["careers_url"]
                changed = True
            if result.get("ir_url") and (not company.ir_url or args.force):
                company.ir_url = result["ir_url"]
                changed = True
            if result.get("blog_url") and (not company.blog_url or args.force):
                company.blog_url = result["blog_url"]
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

            name = company.company_name or company.ticker
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
