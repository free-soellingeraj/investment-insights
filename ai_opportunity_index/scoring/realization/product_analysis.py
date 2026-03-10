"""Sub-scorer B: AI product launches and partnerships from press releases.

Reads pre-collected news articles from local cache (data/raw/news/{TICKER}.json).
Collection is handled by scripts/collect_evidence.py.

Uses Gemini Flash via pydantic_ai with structured output to classify articles.
Returns ClassifiedScorerOutput with cost/revenue/general classification.
"""

import asyncio
import json
import logging

from ai_opportunity_index.config import (
    RAW_DIR,
)
from ai_opportunity_index.prompts.loader import load_prompt
from ai_opportunity_index.scoring.evidence_classification import (
    CaptureStage,
    ClassifiedEvidence,
    ClassifiedScorerOutput,
    TargetDimension,
)

logger = logging.getLogger(__name__)

NEWS_CACHE_DIR = RAW_DIR / "news"

_TARGET_MAP = {
    "cost": TargetDimension.COST,
    "revenue": TargetDimension.REVENUE,
    "general": TargetDimension.GENERAL,
}

_STAGE_MAP = {
    "planned": CaptureStage.PLANNED,
    "invested": CaptureStage.INVESTED,
    "realized": CaptureStage.REALIZED,
}


def _load_cached_articles(ticker: str) -> list[dict] | None:
    """Load pre-collected news articles from local cache."""
    cache_path = NEWS_CACHE_DIR / f"{ticker.upper()}.json"
    if not cache_path.exists():
        return None
    try:
        data = json.loads(cache_path.read_text())
        return data.get("articles", [])
    except Exception as e:
        logger.debug("Failed to read news cache for %s: %s", ticker, e)
        return None


def _get_agent():
    """Lazy-init LLM agent for news extraction."""
    from ai_opportunity_index.llm_backend import get_agent
    from ai_opportunity_index.scoring.pipeline.llm_extractors import ExtractedPassages

    return get_agent(output_type=ExtractedPassages)


def analyze_company_products_classified(
    company_name: str,
    ticker: str,
) -> ClassifiedScorerOutput | None:
    """Analyze a company's AI product activity with cost/revenue/general classification.

    Reads from data/raw/news/{TICKER}.json (collected by collect_evidence.py).
    Uses Gemini Flash to classify each article.

    Returns ClassifiedScorerOutput, or None if no data available.
    """
    articles = _load_cached_articles(ticker)
    if articles is None:
        logger.debug("No cached news for %s; returning None", ticker)
        return None

    if not articles:
        return None

    agent = _get_agent()
    evidence_items: list[ClassifiedEvidence] = []
    cost_score = 0.0
    revenue_score = 0.0
    general_score = 0.0

    for article in articles:
        title = article.get("title", "")
        description = article.get("description", "")
        article_text = f"{title}\n{description}".strip()

        if not article_text or len(article_text) < 20:
            continue

        try:
            prompt = load_prompt(
                "extract_news_evidence",
                company_name=company_name,
                ticker=ticker,
                sector="",
                revenue=0,
                document_text=article_text[:4000],
            )
            result = agent.run_sync(prompt)
            extracted = result.output
        except Exception as e:
            logger.debug("LLM news extraction failed for article '%s': %s", title[:50], e)
            continue

        for p in extracted.passages:
            target = _TARGET_MAP.get(p.target_dimension, TargetDimension.GENERAL)
            stage = _STAGE_MAP.get(p.capture_stage, CaptureStage.INVESTED)
            confidence = max(0.0, min(1.0, p.confidence))

            if target == TargetDimension.COST:
                cost_score += confidence
            elif target == TargetDimension.REVENUE:
                revenue_score += confidence
            else:
                general_score += confidence

            evidence_items.append(ClassifiedEvidence(
                source_type="product",
                target=target,
                stage=stage,
                raw_score=confidence,
                description=title[:200] if title else "News article",
                source_excerpt=p.passage_text[:500],
                metadata={
                    "method": "llm",
                    "reasoning": p.reasoning,
                    "url": article.get("url", ""),
                },
            ))

    # Normalize scores to 0-1 (cap at 1.0)
    cost_score = round(min(1.0, cost_score), 4)
    revenue_score = round(min(1.0, revenue_score), 4)
    general_score = round(min(1.0, general_score), 4)
    overall = round(min(1.0, cost_score + revenue_score + general_score), 4)

    return ClassifiedScorerOutput(
        overall_score=overall,
        cost_capture_score=cost_score,
        revenue_capture_score=revenue_score,
        general_investment_score=general_score,
        evidence_items=evidence_items[:20],
        raw_details={
            "method": "gemini_flash",
            "articles_processed": len(articles),
            "passages_extracted": len(evidence_items),
        },
    )


async def analyze_company_products_classified_async(
    company_name: str,
    ticker: str,
    semaphore: asyncio.Semaphore | None = None,
) -> ClassifiedScorerOutput | None:
    """Async version of analyze_company_products_classified.

    Processes all articles concurrently with asyncio.gather().
    An optional semaphore gates concurrent LLM calls.
    Returns None if no data available.
    """
    articles = _load_cached_articles(ticker)
    if articles is None:
        logger.debug("No cached news for %s; returning None", ticker)
        return None

    if not articles:
        return None

    agent = _get_agent()

    async def _process_article(article: dict) -> list[tuple]:
        """Process a single article and return (target, stage, confidence, evidence) tuples."""
        title = article.get("title", "")
        description = article.get("description", "")
        article_text = f"{title}\n{description}".strip()

        if not article_text or len(article_text) < 20:
            return []

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
                "Product LLM (fallback) [%s] '%s': input=%d output=%d total=%d tokens",
                ticker, title[:40],
                usage.input_tokens or 0, usage.output_tokens or 0, usage.total_tokens or 0,
            )
            extracted = result.output
        except Exception as e:
            logger.debug("LLM news extraction failed for article '%s': %s", title[:50], e)
            return []

        results = []
        for p in extracted.passages:
            target = _TARGET_MAP.get(p.target_dimension, TargetDimension.GENERAL)
            stage = _STAGE_MAP.get(p.capture_stage, CaptureStage.INVESTED)
            confidence = max(0.0, min(1.0, p.confidence))
            results.append((target, stage, confidence, p, title, article.get("url", "")))
        return results

    # Process all articles concurrently
    article_results = await asyncio.gather(
        *[_process_article(a) for a in articles],
        return_exceptions=True,
    )

    evidence_items: list[ClassifiedEvidence] = []
    cost_score = 0.0
    revenue_score = 0.0
    general_score = 0.0

    for result in article_results:
        if isinstance(result, Exception):
            logger.debug("Article processing failed: %s", result)
            continue
        for target, stage, confidence, p, title, url in result:
            if target == TargetDimension.COST:
                cost_score += confidence
            elif target == TargetDimension.REVENUE:
                revenue_score += confidence
            else:
                general_score += confidence

            evidence_items.append(ClassifiedEvidence(
                source_type="product",
                target=target,
                stage=stage,
                raw_score=confidence,
                description=title[:200] if title else "News article",
                source_excerpt=p.passage_text[:500],
                metadata={
                    "method": "llm",
                    "reasoning": p.reasoning,
                    "url": url,
                },
            ))

    # Normalize scores to 0-1 (cap at 1.0)
    cost_score = round(min(1.0, cost_score), 4)
    revenue_score = round(min(1.0, revenue_score), 4)
    general_score = round(min(1.0, general_score), 4)
    overall = round(min(1.0, cost_score + revenue_score + general_score), 4)

    return ClassifiedScorerOutput(
        overall_score=overall,
        cost_capture_score=cost_score,
        revenue_capture_score=revenue_score,
        general_investment_score=general_score,
        evidence_items=evidence_items[:20],
        raw_details={
            "method": "gemini_flash",
            "articles_processed": len(articles),
            "passages_extracted": len(evidence_items),
        },
    )
