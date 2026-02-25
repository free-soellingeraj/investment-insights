"""Fetch SEC EDGAR filings (10-K, 10-Q, 8-K) for companies."""

import json
import logging
import time
from pathlib import Path

import requests

from ai_opportunity_index.config import (
    RAW_DIR,
    SEC_RATE_LIMIT_SECONDS,
    SEC_USER_AGENT,
)

logger = logging.getLogger(__name__)

HEADERS = {"User-Agent": SEC_USER_AGENT}
EDGAR_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik:010d}.json"
EDGAR_FULL_TEXT_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{filename}"


def get_company_filings(cik: int, filing_type: str = "10-K", count: int = 5) -> list[dict]:
    """Fetch recent filing metadata for a company from EDGAR.

    Args:
        cik: Central Index Key for the company.
        filing_type: Type of filing (10-K, 10-Q, 8-K).
        count: Maximum number of filings to return.

    Returns:
        List of dicts with keys: accession_number, filing_date, primary_document, form.
    """
    url = EDGAR_SUBMISSIONS_URL.format(cik=cik)
    time.sleep(SEC_RATE_LIMIT_SECONDS)

    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    recent = data.get("filings", {}).get("recent", {})
    if not recent:
        return []

    forms = recent.get("form", [])
    accessions = recent.get("accessionNumber", [])
    dates = recent.get("filingDate", [])
    primary_docs = recent.get("primaryDocument", [])

    filings = []
    for i, form in enumerate(forms):
        if form == filing_type and len(filings) < count:
            filings.append(
                {
                    "accession_number": accessions[i].replace("-", ""),
                    "accession_raw": accessions[i],
                    "filing_date": dates[i],
                    "primary_document": primary_docs[i],
                    "form": form,
                }
            )

    return filings


def download_filing_text(cik: int, filing: dict) -> str | None:
    """Download the full text of an SEC filing.

    Args:
        cik: Company CIK.
        filing: Dict from get_company_filings().

    Returns:
        Filing text content, or None on failure.
    """
    url = EDGAR_FULL_TEXT_URL.format(
        cik=cik,
        accession=filing["accession_number"],
        filename=filing["primary_document"],
    )
    time.sleep(SEC_RATE_LIMIT_SECONDS)

    try:
        resp = requests.get(url, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        logger.warning("Failed to download filing %s for CIK %d: %s",
                       filing["accession_raw"], cik, e)
        return None


def fetch_and_cache_filings(
    cik: int,
    ticker: str,
    filing_type: str = "10-K",
    count: int = 1,
) -> list[Path]:
    """Fetch filings and cache them locally.

    Returns list of paths to cached filing text files.
    """
    cache_dir = RAW_DIR / "filings" / ticker.upper()
    cache_dir.mkdir(parents=True, exist_ok=True)

    filings = get_company_filings(cik, filing_type=filing_type, count=count)
    paths = []

    for filing in filings:
        filename = f"{filing_type}_{filing['filing_date']}.txt"
        filepath = cache_dir / filename

        if filepath.exists():
            logger.debug("Using cached filing %s", filepath)
            paths.append(filepath)
            continue

        text = download_filing_text(cik, filing)
        if text:
            filepath.write_text(text)
            paths.append(filepath)
            logger.info("Cached filing %s for %s", filename, ticker)

    return paths


def extract_filing_sections(text: str) -> dict[str, str]:
    """Extract key sections from a 10-K filing text.

    Attempts to identify and extract:
    - Business description (Item 1)
    - Risk factors (Item 1A)
    - MD&A (Item 7)

    Returns dict of section_name → section_text.
    """
    import re

    sections = {}
    text_lower = text.lower()

    # Simple heuristic extraction based on item headers
    patterns = {
        "business": r"item\s+1[.\s]+business",
        "risk_factors": r"item\s+1a[.\s]+risk\s+factors",
        "mda": r"item\s+7[.\s]+management.{0,20}discussion",
    }

    found_positions = {}
    for name, pattern in patterns.items():
        match = re.search(pattern, text_lower)
        if match:
            found_positions[name] = match.start()

    # Sort by position and extract text between sections
    sorted_sections = sorted(found_positions.items(), key=lambda x: x[1])
    for i, (name, start) in enumerate(sorted_sections):
        if i + 1 < len(sorted_sections):
            end = sorted_sections[i + 1][1]
        else:
            end = min(start + 50000, len(text))  # cap at 50k chars
        sections[name] = text[start:end][:50000]

    return sections
