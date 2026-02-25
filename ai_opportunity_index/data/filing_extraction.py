"""Pre-extract AI signals from SEC filings and cache to JSON.

Runs Gemini Flash via pydantic_ai for each filing file, then caches
structured extraction results to data/raw/extracted_filings/{TICKER}/{FILENAME}.json.

Each individual filing is cached permanently — SEC filings are static once filed.
The per-company rollup ({TICKER}.json) is rebuilt from per-filing caches.
"""

import asyncio
import json
import logging
import re
from datetime import datetime
from pathlib import Path

from ai_opportunity_index.cache import cache_is_fresh, stamp_cache
from ai_opportunity_index.config import LLM_EXTRACTION_MODEL, RAW_DIR, get_google_provider
from ai_opportunity_index.prompts.loader import load_prompt

logger = logging.getLogger(__name__)

EXTRACTED_FILINGS_DIR = RAW_DIR / "extracted_filings"


def _strip_xbrl_tags(text: str) -> str:
    """Extract readable text from SEC filing HTML/XBRL, stripping all markup."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(text, "html.parser")

    # Focus on <body> content (skips XBRL hidden context elements in <head>)
    body = soup.find("body")
    if body:
        soup = body

    # Remove non-content elements
    for tag in soup(["script", "style", "head"]):
        tag.decompose()
    # Remove XBRL hidden context blocks (display:none)
    for tag in soup.find_all(style=re.compile(r"display\s*:\s*none", re.I)):
        tag.decompose()

    # Get visible text
    visible = soup.get_text(separator=" ", strip=True)
    # Collapse whitespace
    visible = re.sub(r"\s+", " ", visible).strip()
    return visible


def _get_agent():
    """Lazy-init pydantic_ai agent for filing extraction."""
    from pydantic_ai import Agent
    from pydantic_ai.models.google import GoogleModel

    from ai_opportunity_index.scoring.pipeline.llm_extractors import ExtractedPassages

    model = GoogleModel(LLM_EXTRACTION_MODEL, provider=get_google_provider())
    return Agent(model, output_type=ExtractedPassages)


def _per_filing_cache_path(ticker: str, filename: str) -> Path:
    """Return the cache path for a single filing: extracted_filings/{TICKER}/{FILENAME}.json."""
    return EXTRACTED_FILINGS_DIR / ticker.upper() / f"{filename}.json"


def _read_per_filing_cache(ticker: str, filename: str) -> dict | None:
    """Read a single filing's extraction cache. Returns None if not cached."""
    path = _per_filing_cache_path(ticker, filename)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def _write_per_filing_cache(ticker: str, filename: str, data: dict) -> None:
    """Write a single filing's extraction result to cache."""
    path = _per_filing_cache_path(ticker, filename)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))


async def extract_filings_for_company(
    ticker: str,
    company_name: str = "",
    sector: str = "",
    revenue: float = 0,
    employees: int = 0,
    force: bool = False,
    semaphore: asyncio.Semaphore | None = None,
) -> dict:
    """Extract AI signals from all filings for a company.

    Reads filing text files from data/raw/filings/{TICKER}/*.txt.
    Each filing is cached individually and permanently — only new/unprocessed
    filings trigger LLM calls.
    """
    filing_dir = RAW_DIR / "filings" / ticker.upper()
    if not filing_dir.exists():
        return {"ticker": ticker, "extracted_at": datetime.utcnow().isoformat(), "filings": []}

    filing_files = sorted(filing_dir.glob("*.txt"))
    if not filing_files:
        return {"ticker": ticker, "extracted_at": datetime.utcnow().isoformat(), "filings": []}

    agent = None  # lazy-init only if we need LLM calls

    async def _extract_one(filing_path) -> dict | None:
        nonlocal agent

        # Check per-filing cache first (use unified cache for version check)
        per_filing_path = _per_filing_cache_path(ticker, filing_path.name)
        if cache_is_fresh(per_filing_path, "extract_filings", force=force):
            cached = _read_per_filing_cache(ticker, filing_path.name)
            if cached is not None:
                logger.debug("Filing cache hit: %s/%s", ticker, filing_path.name)
                return cached

        raw_text = filing_path.read_text(errors="ignore")
        if len(raw_text) < 100:
            return None

        # Strip XBRL/HTML tags — SEC filings are often raw XML
        text = _strip_xbrl_tags(raw_text)
        if len(text) < 200:
            return None

        try:
            if agent is None:
                agent = _get_agent()

            prompt = load_prompt(
                "extract_filing_evidence",
                company_name=company_name,
                ticker=ticker,
                sector=sector,
                revenue=revenue,
                employees=employees,
                document_text=text,
            )
            if semaphore:
                async with semaphore:
                    result = await agent.run(prompt)
            else:
                result = await agent.run(prompt)

            usage = result.usage()
            logger.info(
                "Filing LLM [%s/%s]: input=%d output=%d total=%d tokens, %d passages",
                ticker, filing_path.name,
                usage.input_tokens or 0,
                usage.output_tokens or 0,
                usage.total_tokens or 0,
                len(result.output.passages),
            )

            passages = []
            for p in result.output.passages:
                passages.append({
                    "passage_text": p.passage_text,
                    "target_dimension": p.target_dimension,
                    "capture_stage": p.capture_stage,
                    "confidence": p.confidence,
                    "reasoning": p.reasoning,
                })

            # Parse filing date from filename (e.g. 10-Q_2023-08-04.txt → 2023-08-04)
            filing_date = ""
            date_match = re.search(r"(\d{4}-\d{2}-\d{2})", filing_path.name)
            if date_match:
                filing_date = date_match.group(1)

            filing_result = {
                "filename": filing_path.name,
                "filing_date": filing_date,
                "passages": passages,
                "extracted_at": datetime.utcnow().isoformat(),
                "input_tokens": usage.input_tokens or 0,
                "output_tokens": usage.output_tokens or 0,
            }

            # Cache per-filing result permanently (only if we got passages,
            # to avoid poisoning cache with rate-limited empty results)
            if passages:
                stamp_cache(filing_result, "extract_filings")
                _write_per_filing_cache(ticker, filing_path.name, filing_result)
            else:
                logger.debug(
                    "Skipping per-filing cache for %s/%s: 0 passages (may be rate-limited)",
                    ticker, filing_path.name,
                )

            return filing_result
        except Exception as e:
            logger.debug("Filing extraction failed for %s/%s: %s", ticker, filing_path.name, e)
            return None

    results = await asyncio.gather(
        *[_extract_one(f) for f in filing_files],
        return_exceptions=True,
    )

    filings = []
    for r in results:
        if isinstance(r, Exception):
            logger.debug("Filing extraction error: %s", r)
            continue
        if r is not None:
            filings.append(r)

    return {
        "ticker": ticker,
        "company_name": company_name,
        "extracted_at": datetime.utcnow().isoformat(),
        "filing_count": len(filings),
        "filings": filings,
    }


async def extract_and_cache_filings(
    ticker: str,
    company_name: str = "",
    sector: str = "",
    revenue: float = 0,
    employees: int = 0,
    force: bool = False,
    semaphore: asyncio.Semaphore | None = None,
) -> dict:
    """Extract filings and write rollup cache to data/raw/extracted_filings/{TICKER}.json.

    Individual filings are cached permanently. The per-company rollup is always
    rebuilt from per-filing caches (cheap — just reads JSON files).
    """
    EXTRACTED_FILINGS_DIR.mkdir(parents=True, exist_ok=True)

    data = await extract_filings_for_company(
        ticker, company_name, sector, revenue, employees,
        force=force,
        semaphore=semaphore,
    )

    # Write rollup cache (always, since it's just an aggregation of per-filing caches)
    rollup_path = EXTRACTED_FILINGS_DIR / f"{ticker.upper()}.json"
    has_passages = any(
        f.get("passages") for f in data.get("filings", [])
    )
    if has_passages or not data.get("filings"):
        stamp_cache(data, "extract_filings")
        rollup_path.write_text(json.dumps(data, indent=2))

    return data
