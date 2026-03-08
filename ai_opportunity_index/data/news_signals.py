"""Fetch press releases and news signals via free APIs/RSS.

Primary source: Google News RSS (free, no API key required).
Optional upgrade: GNews API (free tier: 100 req/day with API key).
Secondary source: SEC EDGAR EFTS full-text search for 8-K press releases.
"""

import logging
import re
import time
import urllib.parse
from datetime import date, datetime, timedelta
from xml.etree import ElementTree

import feedparser
import requests

from ai_opportunity_index.config import RAW_DIR
from ai_opportunity_index.domains import CollectedItem, SourceAuthority

logger = logging.getLogger(__name__)

# Google News RSS (free, no API key)
GOOGLE_NEWS_RSS_SEARCH = "https://news.google.com/rss/search"

# GNews API (optional, requires API key, free tier: 100 req/day)
GNEWS_SEARCH_URL = "https://gnews.io/api/v4/search"

# SEC EDGAR full-text search (free, no API key)
SEC_EFTS_URL = "https://efts.sec.gov/LATEST/search-index"


def search_company_news(
    company_name: str,
    ticker: str,
    days_back: int = 90,
    api_key: str | None = None,
    since_date: datetime | None = None,
) -> list[CollectedItem]:
    """Search for recent news about a company using free APIs.

    Priority:
    1. GNews API (if api_key provided)
    2. Google News RSS (free, no key needed) -- primary free source
    3. SEC EDGAR EFTS 8-K full-text search (supplementary)

    If since_date is set, computes days_back from that date.

    Returns list of CollectedItem objects.
    """
    if since_date:
        days_back = max(1, (datetime.utcnow() - since_date).days)

    articles: list[dict] = []

    # Try GNews first if API key is available
    if api_key:
        articles = _search_gnews(company_name, ticker, days_back, api_key)

    # Google News RSS -- primary free source (no API key needed)
    if not articles:
        articles = _search_google_news_rss(company_name, ticker, days_back)

    # Supplement with SEC EDGAR 8-K press releases mentioning AI
    if len(articles) < 5:
        sec_articles = _search_sec_efts_8k(company_name, ticker, days_back)
        # Deduplicate by title similarity
        existing_titles = {a["title"].lower()[:50] for a in articles}
        for sa in sec_articles:
            if sa["title"].lower()[:50] not in existing_titles:
                articles.append(sa)

    # Convert to CollectedItem
    today = date.today()
    items: list[CollectedItem] = []
    for a in articles:
        source_date = _parse_article_date(a.get("published_at"))
        source_name = a.get("source", "")
        items.append(CollectedItem(
            item_id=a.get("url") or f"{ticker}_{a.get('title', '')[:50]}",
            title=a.get("title", ""),
            content=f"{a.get('title', '')}\n{a.get('description', '')}".strip(),
            author=source_name or None,  # RSS doesn't give individual bylines
            author_role="journalist" if source_name else None,
            author_affiliation=source_name or None,
            publisher=source_name or None,
            url=a.get("url"),
            source_date=source_date,
            access_date=today,
            authority=SourceAuthority.THIRD_PARTY_JOURNALISM,
            metadata={
                "raw_title": a.get("title", ""),
                "raw_published": a.get("published_at", ""),
                "description": a.get("description", ""),
            },
        ))
    return items


def _parse_article_date(s: str | None) -> date | None:
    """Parse a date string from news article, returning None on failure."""
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except (ValueError, TypeError):
        pass
    try:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").date()
    except (ValueError, TypeError):
        return None


def _search_google_news_rss(
    company_name: str,
    ticker: str,
    days_back: int,
) -> list[dict]:
    """Search Google News RSS feed for company AI news.

    Free, no API key required. Returns up to 100 articles.
    Uses the when: parameter for time filtering and adds AI-related
    search terms to find relevant product/partnership news.
    """
    # Build search query: company name OR ticker, combined with AI keywords
    # Use quotes for exact company name match to reduce noise
    company_clean = company_name.replace('"', "").strip()
    # Cap days_back for the when: param (Google News supports up to ~12 months)
    when_days = min(days_back, 365)

    # Two queries: one broad AI query, one for partnerships
    queries = [
        f'"{company_clean}" AI when:{when_days}d',
        f'"{company_clean}" ("artificial intelligence" OR "machine learning" OR "generative AI") when:{when_days}d',
    ]

    all_articles = []
    seen_urls = set()

    for query in queries:
        params = {
            "q": query,
            "hl": "en-US",
            "gl": "US",
            "ceid": "US:en",
        }
        url = f"{GOOGLE_NEWS_RSS_SEARCH}?{urllib.parse.urlencode(params)}"

        try:
            feed = feedparser.parse(
                url,
                request_headers={"User-Agent": "Mozilla/5.0"},
            )

            if feed.bozo and not feed.entries:
                logger.warning(
                    "Google News RSS parse error for %s: %s",
                    company_name,
                    feed.bozo_exception,
                )
                continue

            for entry in feed.entries[:50]:  # Cap per query
                article_url = entry.get("link", "")
                if article_url in seen_urls:
                    continue
                seen_urls.add(article_url)

                # Parse publication date
                published = entry.get("published", "")
                if hasattr(entry, "published_parsed") and entry.published_parsed:
                    try:
                        published = datetime(
                            *entry.published_parsed[:6]
                        ).strftime("%Y-%m-%dT%H:%M:%SZ")
                    except Exception:
                        pass

                # Google News RSS provides title and a brief description/summary
                title = entry.get("title", "")
                # The description in Google News RSS often contains HTML with
                # links to the source; extract plain text
                description = _strip_html(entry.get("summary", entry.get("description", "")))

                # Extract source name from title (Google News format: "Title - Source")
                source = ""
                if " - " in title:
                    parts = title.rsplit(" - ", 1)
                    source = parts[-1].strip()

                all_articles.append({
                    "title": title,
                    "description": description,
                    "url": article_url,
                    "published_at": published,
                    "source": source,
                })

            # Rate limit between queries
            time.sleep(0.5)

        except Exception as e:
            logger.warning("Google News RSS search failed for %s: %s", company_name, e)

    logger.info(
        "Google News RSS: found %d articles for %s (%s)",
        len(all_articles),
        company_name,
        ticker,
    )
    return all_articles


def _strip_html(text: str) -> str:
    """Remove HTML tags from text."""
    clean = re.sub(r"<[^>]+>", " ", text)
    clean = re.sub(r"\s+", " ", clean)
    return clean.strip()


def _search_gnews(
    company_name: str,
    ticker: str,
    days_back: int,
    api_key: str,
) -> list[dict]:
    """Search GNews API for company news (requires API key)."""
    query = f'"{company_name}" OR "{ticker}" AI artificial intelligence'
    params = {
        "q": query,
        "lang": "en",
        "max": 10,
        "apikey": api_key,
    }

    try:
        resp = requests.get(GNEWS_SEARCH_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        return [
            {
                "title": a.get("title", ""),
                "description": a.get("description", ""),
                "url": a.get("url", ""),
                "published_at": a.get("publishedAt", ""),
                "source": a.get("source", {}).get("name", ""),
            }
            for a in data.get("articles", [])
        ]
    except Exception as e:
        logger.warning("GNews search failed for %s: %s", company_name, e)
        return []


def _search_sec_efts_8k(
    company_name: str,
    ticker: str,
    days_back: int,
) -> list[dict]:
    """Search SEC EDGAR EFTS for 8-K filings mentioning AI topics.

    8-K filings often contain press releases (Exhibit 99.1) about
    product launches, partnerships, and strategic announcements.
    Free, no API key required.
    """
    start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")

    # Search for 8-K filings that mention AI terms in their full text
    # The EFTS endpoint accepts query string params for simple searches
    query = f'"{ticker}" ("artificial intelligence" OR "machine learning" OR "generative AI" OR "AI-powered")'

    params = {
        "q": query,
        "dateRange": "custom",
        "startdt": start_date,
        "enddt": end_date,
        "forms": "8-K",
    }

    try:
        resp = requests.get(
            SEC_EFTS_URL,
            params=params,
            headers={"User-Agent": "AIOpportunityIndex research@example.com"},
            timeout=15,
        )

        if resp.status_code != 200:
            logger.debug(
                "SEC EFTS returned status %d for %s", resp.status_code, ticker
            )
            return []

        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])

        articles = []
        for h in hits[:10]:
            source = h.get("_source", {})
            # 8-K filings have display_names (company), form_type, file_date
            # and sometimes entity_name or file_description with more context
            company_names = source.get("display_names", [])
            form_type = source.get("form_type", "8-K")
            file_date = source.get("file_date", "")
            file_desc = source.get("file_description", "")

            # Build a descriptive title from available metadata
            company_display = company_names[0] if company_names else ticker
            title = f"{company_display} - {form_type} Filing"
            if file_desc:
                title = f"{company_display}: {file_desc}"

            articles.append({
                "title": title,
                "description": f"SEC {form_type} filing dated {file_date}. "
                               f"Filing contains AI-related content.",
                "url": "",
                "published_at": file_date,
                "source": "SEC EDGAR",
            })

        logger.info(
            "SEC EFTS: found %d 8-K AI filings for %s", len(articles), ticker
        )
        return articles

    except Exception as e:
        logger.debug("SEC EFTS search failed for %s: %s", ticker, e)
        return []


def classify_ai_relevance(articles: list[dict]) -> dict:
    """Classify news articles for AI relevance using keyword matching.

    Returns dict with: ai_article_count, total_articles, ai_relevance_ratio,
    ai_keywords_found.
    """
    ai_keywords = [
        r"\bai\b", r"\bartificial intelligence\b", r"\bmachine learning\b",
        r"\bdeep learning\b", r"\bneural network\b", r"\bgenerative ai\b",
        r"\bllm\b", r"\blarge language model\b", r"\bchatbot\b",
        r"\bcopilot\b", r"\bgpt\b", r"\bautomation\b", r"\brobotics\b",
        r"\bcomputer vision\b", r"\bnlp\b", r"\bnatural language\b",
    ]

    ai_count = 0
    keywords_found = set()

    for article in articles:
        text = f"{article.get('title', '')} {article.get('description', '')}".lower()
        for kw in ai_keywords:
            if re.search(kw, text):
                ai_count += 1
                keywords_found.add(kw.strip(r"\b"))
                break

    total = len(articles)
    return {
        "ai_article_count": ai_count,
        "total_articles": total,
        "ai_relevance_ratio": ai_count / total if total > 0 else 0.0,
        "ai_keywords_found": list(keywords_found),
    }
