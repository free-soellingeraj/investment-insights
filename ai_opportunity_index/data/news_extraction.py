"""Pre-extract AI signals from news articles and cache to JSON.

Runs Gemini Flash via pydantic_ai for each news article, then caches
structured extraction results to data/raw/extracted_news/{TICKER}.json.
Scoring can then read the cache instead of making live LLM calls.
"""

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path

from ai_opportunity_index.cache import cache_is_fresh, stamp_cache
from ai_opportunity_index.config import LLM_EXTRACTION_MODEL, RAW_DIR, get_google_provider
from ai_opportunity_index.prompts.loader import load_prompt

logger = logging.getLogger(__name__)

NEWS_CACHE_DIR = RAW_DIR / "news"
EXTRACTED_NEWS_DIR = RAW_DIR / "extracted_news"


def _get_agent():
    """Lazy-init pydantic_ai agent for news extraction."""
    from pydantic_ai import Agent
    from pydantic_ai.models.google import GoogleModel

    from ai_opportunity_index.scoring.pipeline.llm_extractors import ExtractedPassages

    model = GoogleModel(LLM_EXTRACTION_MODEL, provider=get_google_provider())
    return Agent(model, output_type=ExtractedPassages)


async def extract_news_for_company(
    ticker: str,
    company_name: str = "",
    semaphore: asyncio.Semaphore | None = None,
) -> dict:
    """Extract AI signals from all cached news articles for a company.

    Reads articles from data/raw/news/{TICKER}.json,
    runs Gemini Flash on each, and returns structured extraction results.
    """
    news_path = NEWS_CACHE_DIR / f"{ticker.upper()}.json"
    if not news_path.exists():
        return {"ticker": ticker, "extracted_at": datetime.utcnow().isoformat(), "articles": []}

    try:
        raw_data = json.loads(news_path.read_text())
        articles = raw_data.get("articles", [])
    except Exception as e:
        logger.debug("Failed to read news cache for %s: %s", ticker, e)
        return {"ticker": ticker, "extracted_at": datetime.utcnow().isoformat(), "articles": []}

    if not articles:
        return {"ticker": ticker, "extracted_at": datetime.utcnow().isoformat(), "articles": []}

    agent = _get_agent()

    async def _extract_one(article: dict) -> dict | None:
        title = article.get("title", "")
        description = article.get("description", "")
        article_text = f"{title}\n{description}".strip()

        if not article_text or len(article_text) < 20:
            return None

        try:
            prompt = load_prompt(
                "extract_news_evidence",
                company_name=company_name,
                ticker=ticker,
                sector="",
                revenue=0,
                document_text=article_text[:4000],
            )
            if semaphore:
                async with semaphore:
                    result = await agent.run(prompt)
            else:
                result = await agent.run(prompt)

            usage = result.usage()
            logger.info(
                "News LLM [%s] '%s': input=%d output=%d total=%d tokens, %d passages",
                ticker, title[:40],
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

            return {
                "title": title,
                "url": article.get("url", ""),
                "published_at": article.get("published_at", ""),
                "source": article.get("source", ""),
                "passages": passages,
            }
        except Exception as e:
            logger.debug("News extraction failed for '%s': %s", title[:50], e)
            return None

    results = await asyncio.gather(
        *[_extract_one(a) for a in articles],
        return_exceptions=True,
    )

    extracted_articles = []
    for r in results:
        if isinstance(r, Exception):
            logger.debug("News extraction error: %s", r)
            continue
        if r is not None:
            extracted_articles.append(r)

    return {
        "ticker": ticker,
        "company_name": company_name,
        "extracted_at": datetime.utcnow().isoformat(),
        "articles_input": len(articles),
        "articles_extracted": len(extracted_articles),
        "articles": extracted_articles,
    }


async def extract_and_cache_news(
    ticker: str,
    company_name: str = "",
    force: bool = False,
    semaphore: asyncio.Semaphore | None = None,
) -> dict:
    """Extract news and write cache to data/raw/extracted_news/{TICKER}.json."""
    EXTRACTED_NEWS_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = EXTRACTED_NEWS_DIR / f"{ticker.upper()}.json"

    if cache_is_fresh(cache_path, "extract_news", force=force):
        return json.loads(cache_path.read_text())

    data = await extract_news_for_company(
        ticker, company_name, semaphore=semaphore,
    )

    # Only cache if we got actual passages — don't poison cache with empty results
    has_passages = any(
        a.get("passages") for a in data.get("articles", [])
    )
    if has_passages or not data.get("articles_input", 0):
        stamp_cache(data, "extract_news")
        cache_path.write_text(json.dumps(data, indent=2))
    else:
        logger.warning(
            "Skipping cache for %s: %d articles processed but 0 passages extracted (likely rate-limited)",
            ticker, data.get("articles_input", 0),
        )

    return data
