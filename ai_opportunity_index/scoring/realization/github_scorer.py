"""Sub-scorer D: GitHub AI/ML activity signals.

Reads pre-collected GitHub data from local cache (data/raw/github/{TICKER}.json).
Collection is handled by scripts/collect_evidence.py --sources github.

Uses heuristic scoring based on AI repo count, stars, and recent activity.
Returns ClassifiedScorerOutput with cost/revenue/general classification.
"""

import json
import logging

from ai_opportunity_index.config import RAW_DIR
from ai_opportunity_index.scoring.evidence_classification import (
    CaptureStage,
    ClassifiedEvidence,
    ClassifiedScorerOutput,
    TargetDimension,
)

logger = logging.getLogger(__name__)

GITHUB_CACHE_DIR = RAW_DIR / "github"


def score_github_classified(
    company_name: str,
    ticker: str,
) -> ClassifiedScorerOutput | None:
    """Score GitHub AI activity for a company.

    Reads data/raw/github/{TICKER}.json, applies heuristics to compute
    cost/revenue/general scores based on:
    - Number of AI/ML repos (signals invested stage)
    - Stars on AI repos (signals realized/community validation)
    - Recent activity (signals active investment)

    Returns ClassifiedScorerOutput or None if no data.
    """
    cache_file = GITHUB_CACHE_DIR / f"{ticker.upper()}.json"
    if not cache_file.exists():
        logger.debug("No GitHub cache for %s", ticker)
        return None

    try:
        data = json.loads(cache_file.read_text())
    except Exception as e:
        logger.warning("Failed to read GitHub cache for %s: %s", ticker, e)
        return None

    ai_repos = data.get("ai_ml_repos", 0)
    ai_stars = data.get("ai_ml_stars", 0)
    total_repos = data.get("total_repos", 0)
    recent_activity = data.get("recent_commits_30d", 0)
    top_ai_repos = data.get("top_ai_repos", [])
    org_name = data.get("org_name")

    if not org_name or ai_repos == 0:
        return None

    evidence_items: list[ClassifiedEvidence] = []

    # AI repo presence → general investment signal
    # More repos = higher investment score (diminishing returns at 10+)
    repo_score = min(ai_repos / 10.0, 1.0)
    if ai_repos > 0:
        evidence_items.append(ClassifiedEvidence(
            source_type="github",
            target=TargetDimension.GENERAL,
            stage=CaptureStage.INVESTED,
            raw_score=repo_score,
            description=f"{ai_repos} AI/ML repos on GitHub ({org_name})",
            metadata={"ai_repos": ai_repos, "total_repos": total_repos},
        ))

    # Stars → realized/community validation → revenue signal
    # High star count suggests the company is producing AI tools others use
    star_score = min(ai_stars / 5000.0, 1.0) if ai_stars > 0 else 0.0
    if star_score > 0:
        stage = CaptureStage.REALIZED if ai_stars > 1000 else CaptureStage.INVESTED
        evidence_items.append(ClassifiedEvidence(
            source_type="github",
            target=TargetDimension.REVENUE,
            stage=stage,
            raw_score=star_score,
            description=f"{ai_stars} stars across AI/ML repos",
            metadata={"ai_stars": ai_stars},
        ))

    # Recent activity → ongoing investment (cost dimension — active development)
    activity_score = min(recent_activity / 5.0, 1.0) if recent_activity > 0 else 0.0
    if activity_score > 0:
        evidence_items.append(ClassifiedEvidence(
            source_type="github",
            target=TargetDimension.COST,
            stage=CaptureStage.INVESTED,
            raw_score=activity_score,
            description=f"{recent_activity} recently active AI repos (30d)",
            metadata={"recent_commits_30d": recent_activity},
        ))

    # Top repos → specific evidence items
    for repo in top_ai_repos[:3]:
        repo_stars = repo.get("stars", 0)
        repo_name = repo.get("name", "")
        repo_desc = repo.get("description", "")
        if repo_stars > 100:
            evidence_items.append(ClassifiedEvidence(
                source_type="github",
                target=TargetDimension.GENERAL,
                stage=CaptureStage.REALIZED if repo_stars > 1000 else CaptureStage.INVESTED,
                raw_score=min(repo_stars / 2000.0, 1.0),
                description=f"{repo_name}: {repo_desc[:100]}" if repo_desc else repo_name,
                source_excerpt=f"{repo_stars} stars, lang: {repo.get('language', 'N/A')}",
                metadata={"repo_name": repo_name, "stars": repo_stars},
            ))

    if not evidence_items:
        return None

    # Aggregate per-dimension scores
    cost_scores = [e.raw_score for e in evidence_items if e.target == TargetDimension.COST]
    revenue_scores = [e.raw_score for e in evidence_items if e.target == TargetDimension.REVENUE]
    general_scores = [e.raw_score for e in evidence_items if e.target == TargetDimension.GENERAL]

    cost_capture = round(min(1.0, sum(cost_scores)), 4)
    revenue_capture = round(min(1.0, sum(revenue_scores)), 4)
    general_investment = round(min(1.0, sum(general_scores)), 4)
    overall = round(min(1.0, cost_capture + revenue_capture + general_investment), 4)

    return ClassifiedScorerOutput(
        overall_score=overall,
        cost_capture_score=cost_capture,
        revenue_capture_score=revenue_capture,
        general_investment_score=general_investment,
        evidence_items=evidence_items[:20],
        raw_details={
            "org_name": org_name,
            "ai_repos": ai_repos,
            "ai_stars": ai_stars,
            "recent_activity": recent_activity,
        },
    )
