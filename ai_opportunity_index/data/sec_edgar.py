"""Fetch SEC EDGAR filings (10-K, 10-Q, 8-K) for companies."""

import json
import logging
import time
from datetime import date, datetime
from pathlib import Path

import requests

from ai_opportunity_index.config import (
    RAW_DIR,
    SEC_RATE_LIMIT_SECONDS,
    SEC_USER_AGENT,
)
from ai_opportunity_index.domains import CollectedItem, SourceAuthority

logger = logging.getLogger(__name__)

HEADERS = {"User-Agent": SEC_USER_AGENT}
EDGAR_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik:010d}.json"
EDGAR_FULL_TEXT_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{filename}"


def get_company_filings(
    cik: int,
    filing_type: str = "10-K",
    count: int = 5,
    since_date: datetime | None = None,
) -> list[dict]:
    """Fetch recent filing metadata for a company from EDGAR.

    Args:
        cik: Central Index Key for the company.
        filing_type: Type of filing (10-K, 10-Q, 8-K).
        count: Maximum number of filings to return.
        since_date: If set, only return filings filed after this date.

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

    since_str = since_date.strftime("%Y-%m-%d") if since_date else None

    filings = []
    for i, form in enumerate(forms):
        if form == filing_type and len(filings) < count:
            filing_date_str = dates[i]
            if since_str and filing_date_str < since_str:
                continue
            filings.append(
                {
                    "accession_number": accessions[i].replace("-", ""),
                    "accession_raw": accessions[i],
                    "filing_date": filing_date_str,
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
    Also writes a _metadata.json sidecar with CIK, accession numbers,
    and SEC EDGAR URLs so downstream extraction can populate source_url.
    """
    cache_dir = RAW_DIR / "filings" / ticker.upper()
    cache_dir.mkdir(parents=True, exist_ok=True)

    filings = get_company_filings(cik, filing_type=filing_type, count=count)
    paths = []

    # Build metadata index: filename -> filing metadata
    metadata_path = cache_dir / "_metadata.json"
    existing_metadata: dict = {}
    if metadata_path.exists():
        try:
            existing_metadata = json.loads(metadata_path.read_text())
        except Exception:
            pass

    for filing in filings:
        filename = f"{filing_type}_{filing['filing_date']}.txt"
        filepath = cache_dir / filename

        # Always update metadata even for cached filings
        accession = filing["accession_number"]
        primary_doc = filing.get("primary_document", "")
        edgar_url = EDGAR_FULL_TEXT_URL.format(
            cik=cik, accession=accession, filename=primary_doc,
        )
        existing_metadata[filename] = {
            "cik": cik,
            "accession_number": accession,
            "accession_raw": filing.get("accession_raw", ""),
            "primary_document": primary_doc,
            "filing_date": filing["filing_date"],
            "form": filing.get("form", filing_type),
            "url": edgar_url,
        }

        if filepath.exists():
            logger.debug("Using cached filing %s", filepath)
            paths.append(filepath)
            continue

        text = download_filing_text(cik, filing)
        if text:
            filepath.write_text(text)
            paths.append(filepath)
            logger.info("Cached filing %s for %s", filename, ticker)

    # Write metadata sidecar
    try:
        metadata_path.write_text(json.dumps(existing_metadata, indent=2))
    except Exception as e:
        logger.warning("Failed to write filing metadata for %s: %s", ticker, e)

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


def filing_to_collected_item(
    filing_meta: dict,
    cik: int,
    ticker: str,
    company_name: str,
    content: str | None = None,
) -> CollectedItem:
    """Convert a filing metadata dict + text to a CollectedItem."""
    accession = filing_meta.get("accession_raw", filing_meta.get("accession_number", ""))
    filing_date_str = filing_meta.get("filing_date", "")
    form_type = filing_meta.get("form", "10-K")
    primary_doc = filing_meta.get("primary_document", "")

    source_date = None
    if filing_date_str:
        try:
            source_date = date.fromisoformat(filing_date_str[:10])
        except (ValueError, TypeError):
            pass

    url = ""
    if cik and accession:
        acc_num = accession.replace("-", "")
        url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_num}/{primary_doc}"

    return CollectedItem(
        item_id=accession or f"{ticker}_{form_type}_{filing_date_str}",
        title=f"{form_type} Filing ({filing_date_str})",
        content=content,
        author=company_name,
        author_role="corporate filer",
        author_affiliation=company_name,
        publisher="SEC EDGAR",
        url=url or None,
        source_date=source_date,
        access_date=date.today(),
        authority=SourceAuthority.FIRST_PARTY_DISCLOSURE,
        metadata={
            "filing_type": form_type,
            "accession_number": accession,
            "cik": cik,
            "primary_document": primary_doc,
        },
    )
