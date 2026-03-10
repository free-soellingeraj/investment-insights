"""Web enrichment — scrape careers, IR, and blog pages for AI signals.

Uses requests + BeautifulSoup to fetch pages, condenses text, then uses
Gemini Flash via pydantic_ai for structured extraction of AI evidence
classified by target dimension (cost/revenue/general) and capture stage
(planned/invested/realized).
"""

import json
import logging
import re as _re
import time
from datetime import date, datetime
from pathlib import Path
from typing import Literal, Optional

from pydantic import BaseModel, Field

from ai_opportunity_index.config import (
    LLM_EXTRACTION_MODEL,
    RAW_DIR,
    WEB_SCRAPE_RATE_LIMIT_SECONDS,
    get_google_provider,
)
from ai_opportunity_index.domains import CollectedItem, SourceAuthority, SourceType

logger = logging.getLogger(__name__)

WEB_ENRICHMENT_CACHE_DIR = RAW_DIR / "web_enrichment"

# ── Pydantic Output Models ────────────────────────────────────────────────


class WebEvidenceItem(BaseModel):
    """A single piece of evidence extracted from a web page."""
    passage_text: str  # verbatim excerpt or summary of what was found
    target_dimension: Literal["cost", "revenue", "general"]
    capture_stage: Literal["planned", "invested", "realized"]
    confidence: float  # 0-1
    reasoning: str  # why this is evidence of AI cost/revenue activity


class WebEvidence(BaseModel):
    """Structured extraction result from a single web page."""
    evidence_items: list[WebEvidenceItem] = Field(default_factory=list)
    page_summary: str = ""  # brief summary of what was on the page


# ── Extraction Prompts ────────────────────────────────────────────────────

CAREERS_PROMPT = """\
You are an expert analyst identifying evidence of AI investment from a company's careers page.

For each relevant signal you find, provide:
- **passage_text**: The verbatim excerpt from the page (max 300 characters) — copy the exact text, \
do not paraphrase. This should be the job title, description snippet, or qualification that shows AI activity.
- **target_dimension**: "cost" if the role automates internal processes (MLOps, automation engineer, RPA, \
AI operations); "revenue" if it builds AI products/services for customers (AI product manager, \
AI solutions architect, generative AI engineer); "general" if unclear.
- **capture_stage**: Always "invested" — active hiring represents committed spending.
- **confidence**: 0.0-1.0 reflecting how clearly this is AI-related hiring.
- **reasoning**: Brief explanation of your classification.

Provide a brief page_summary of overall hiring activity.
"""

IR_PROMPT = """\
You are an expert financial analyst extracting AI investment evidence from a company's investor relations page.

For each relevant signal you find, provide:
- **passage_text**: The verbatim excerpt from the page (max 300 characters) — copy the exact text, \
do not paraphrase. This should be the sentence or phrase that demonstrates the AI activity.
- **target_dimension**: "cost" if about AI reducing internal costs or improving efficiency; \
"revenue" if about AI products/services generating revenue; "general" if broad AI strategy/R&D.
- **capture_stage**: "planned" for announced intentions or strategy; "invested" for reported \
spending or partnerships; "realized" for reported savings, revenue, or measurable results.
- **confidence**: 0.0-1.0 reflecting how specific and credible the evidence is.
- **reasoning**: Brief explanation of your classification.

Look for: AI strategy announcements, AI spending/investment figures, AI revenue or savings metrics, \
AI partnerships, AI product launches mentioned in press releases.
Provide a brief page_summary of the IR page content.
"""

BLOG_PROMPT = """\
You are an expert technology analyst extracting AI investment evidence from a company's blog or newsroom.

For each relevant signal you find, provide:
- **passage_text**: The verbatim excerpt from the page (max 300 characters) — copy the exact text, \
do not paraphrase. This should be the sentence or phrase that demonstrates the AI activity.
- **target_dimension**: "cost" if about internal AI automation or efficiency gains; \
"revenue" if about AI-powered products, features, or services for customers; "general" if broad AI commentary.
- **capture_stage**: "planned" for announced plans or upcoming features; "invested" for launched products \
or deployed internal tools; "realized" for reported metrics, adoption numbers, or revenue/savings figures.
- **confidence**: 0.0-1.0 reflecting how concrete and specific the evidence is.
- **reasoning**: Brief explanation of your classification.

Look for: AI product launches, AI feature announcements, AI efficiency improvements, \
AI adoption metrics, customer success stories with AI.
Provide a brief page_summary of the blog content.
"""

# ── HTTP Scraping ─────────────────────────────────────────────────────────

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

MAX_TEXT_CHARS = 15_000


def _extract_page_date(response, soup) -> tuple[date | None, dict]:
    """Try to extract the content creation date from HTTP headers and HTML meta."""
    meta = {}
    meta["http_last_modified"] = response.headers.get("Last-Modified")
    meta["http_date"] = response.headers.get("Date")

    # Open Graph / article meta tags
    for tag in soup.find_all("meta", property=_re.compile(r"(og:published_time|article:published_time)")):
        meta["og_published_time"] = tag.get("content")
    # JSON-LD structured data
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script.string)
            if isinstance(ld, dict) and "datePublished" in ld:
                meta["jsonld_date_published"] = ld["datePublished"]
        except (json.JSONDecodeError, TypeError):
            pass
    # URL date pattern (e.g. /2024/03/post-title)
    m = _re.search(r"/(\d{4})/(\d{2})/", str(response.url))
    if m:
        meta["url_date_pattern"] = f"{m.group(1)}-{m.group(2)}"

    # Priority: JSON-LD > OG > URL pattern > Last-Modified
    for key in ("jsonld_date_published", "og_published_time", "url_date_pattern", "http_last_modified"):
        if meta.get(key):
            try:
                return date.fromisoformat(str(meta[key])[:10]), meta
            except (ValueError, TypeError):
                pass
    return None, meta


def _fetch_and_condense(url: str) -> str | None:
    """Fetch a URL and return condensed visible text, or None on failure."""
    import requests
    from bs4 import BeautifulSoup

    try:
        resp = requests.get(url, headers=_HEADERS, timeout=20, allow_redirects=True)
        resp.raise_for_status()
    except Exception as e:
        logger.warning("HTTP fetch failed for %s: %s", url, e)
        return None

    soup = BeautifulSoup(resp.text, "lxml")

    # Remove non-content elements
    for tag in soup(["script", "style", "nav", "footer", "header", "noscript", "svg", "iframe"]):
        tag.decompose()

    text = soup.get_text(separator="\n", strip=True)

    # Collapse whitespace
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    text = "\n".join(lines)

    # Truncate
    if len(text) > MAX_TEXT_CHARS:
        text = text[:MAX_TEXT_CHARS]

    return text if text else None


def _fetch_with_metadata(url: str) -> tuple[str | None, date | None, dict]:
    """Fetch a URL, return (condensed_text, source_date, date_metadata)."""
    import requests as _requests
    from bs4 import BeautifulSoup

    try:
        resp = _requests.get(url, headers=_HEADERS, timeout=20, allow_redirects=True)
        resp.raise_for_status()
    except Exception as e:
        logger.warning("HTTP fetch failed for %s: %s", url, e)
        return None, None, {}

    soup = BeautifulSoup(resp.text, "lxml")
    source_date, date_meta = _extract_page_date(resp, soup)

    # Remove non-content elements
    for tag in soup(["script", "style", "nav", "footer", "header", "noscript", "svg", "iframe"]):
        tag.decompose()

    text = soup.get_text(separator="\n", strip=True)
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    text = "\n".join(lines)
    if len(text) > MAX_TEXT_CHARS:
        text = text[:MAX_TEXT_CHARS]

    return (text if text else None), source_date, date_meta


# ── LLM Extraction ───────────────────────────────────────────────────────

def _extract_structured(text: str, prompt: str, output_type: type) -> object | None:
    """Use LLM to extract structured data from text.

    Uses an explicit new event loop to avoid 'bound to a different event loop'
    errors when called from asyncio.to_thread inside the pipeline.
    """
    import asyncio as _asyncio

    from ai_opportunity_index.llm_backend import get_agent

    agent = get_agent(output_type=output_type)

    full_prompt = f"{prompt}\n\n--- PAGE CONTENT ---\n{text}"

    try:
        # Create a fresh event loop for this thread to avoid cross-loop
        # conflicts with pydantic-ai's internal asyncio primitives.
        loop = _asyncio.new_event_loop()
        try:
            result = loop.run_until_complete(agent.run(full_prompt))
        finally:
            loop.close()
        usage = result.usage()
        logger.info(
            "Web enrichment LLM: input=%d output=%d total=%d tokens",
            usage.input_tokens or 0,
            usage.output_tokens or 0,
            usage.total_tokens or 0,
        )
        return result.output
    except Exception as e:
        logger.warning("LLM extraction failed: %s", e)
        return None


# ── Per-Page Scrapers ─────────────────────────────────────────────────────

def _scrape_page(url: str, prompt: str) -> dict | None:
    """Scrape a URL, extract structured evidence, return result dict."""
    time.sleep(WEB_SCRAPE_RATE_LIMIT_SECONDS)

    text = _fetch_and_condense(url)
    if not text:
        return None

    extracted = _extract_structured(text, prompt, WebEvidence)
    if not extracted:
        return None

    return {
        "url": url,
        "scraped_at": datetime.utcnow().isoformat(),
        "metadata": {"text_length": len(text)},
        "markdown_length": len(text),
        "evidence_items": [item.model_dump() for item in extracted.evidence_items],
        "page_summary": extracted.page_summary,
    }


def scrape_careers_page(url: str) -> dict | None:
    """Scrape a careers page for AI hiring signals."""
    return _scrape_page(url, CAREERS_PROMPT)


def scrape_ir_page(url: str) -> dict | None:
    """Scrape an investor relations page for AI strategy signals."""
    return _scrape_page(url, IR_PROMPT)


def scrape_blog_page(url: str) -> dict | None:
    """Scrape a blog page for AI product announcements."""
    return _scrape_page(url, BLOG_PROMPT)


# ── Main Entry Point ─────────────────────────────────────────────────────

def fetch_web_enrichment(
    ticker: str,
    company_name: str,
    careers_url: str | None = None,
    ir_url: str | None = None,
    blog_url: str | None = None,
) -> dict:
    """Fetch web enrichment data for a company.

    Returns a dict with careers, investor_relations, and blog sub-keys,
    each containing evidence_items and page_summary, or None.
    """
    result = {
        "ticker": ticker,
        "company_name": company_name,
        "collected_at": datetime.utcnow().isoformat(),
        "careers": None,
        "investor_relations": None,
        "blog": None,
    }

    if careers_url:
        logger.debug("Scraping careers page for %s: %s", ticker, careers_url)
        result["careers"] = scrape_careers_page(careers_url)

    if ir_url:
        logger.debug("Scraping IR page for %s: %s", ticker, ir_url)
        result["investor_relations"] = scrape_ir_page(ir_url)

    if blog_url:
        logger.debug("Scraping blog page for %s: %s", ticker, blog_url)
        result["blog"] = scrape_blog_page(blog_url)

    return result


# ── CollectedItem Conversion ────────────────────────────────────────────


_WEB_SOURCE_TYPE_MAP = {
    "careers": SourceType.WEB_CAREERS,
    "ir": SourceType.WEB_IR,
    "blog": SourceType.WEB_BLOG,
}


def web_page_to_collected_item(
    url: str,
    page_type: str,  # "careers", "ir", or "blog"
    ticker: str,
    company_name: str,
) -> CollectedItem | None:
    """Fetch a web page and return a CollectedItem with content and date metadata.

    Returns None if the page cannot be fetched.
    """
    text, source_date, date_meta = _fetch_with_metadata(url)
    if not text:
        return None

    today = date.today()
    return CollectedItem(
        item_id=url,
        title=f"{company_name} {page_type} page",
        content=text,
        author=company_name,
        author_role="corporate communications",
        author_affiliation=company_name,
        publisher=company_name,
        url=url,
        source_date=source_date,
        access_date=today,
        authority=SourceAuthority.FIRST_PARTY_PUBLIC,
        metadata={
            "page_type": page_type,
            "text_length": len(text),
            **{k: v for k, v in date_meta.items() if v is not None},
        },
    )
