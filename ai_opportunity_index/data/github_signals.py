"""Fetch GitHub organization and repository signals for AI/ML activity.

Uses the GitHub REST API to find company orgs and their AI-related repos.
No auth required for public data (60 req/hr); with GITHUB_TOKEN: 5000 req/hr.
"""

import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests

from ai_opportunity_index.config import (
    GITHUB_RATE_LIMIT_SECONDS,
    GITHUB_TOKEN,
    RAW_DIR,
)

logger = logging.getLogger(__name__)

GITHUB_API = "https://api.github.com"

AI_ML_KEYWORDS = {
    "ai", "ml", "machine-learning", "deep-learning", "llm",
    "neural", "transformer", "gpt", "diffusion", "nlp",
    "computer-vision", "artificial-intelligence", "generative-ai",
}


def _headers() -> dict:
    h = {"Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        h["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return h


def _is_ai_repo(repo: dict) -> bool:
    """Check if a repo is AI/ML related based on name, description, and topics."""
    text = " ".join([
        (repo.get("name") or "").lower().replace("-", " ").replace("_", " "),
        (repo.get("description") or "").lower(),
    ])
    topics = {t.lower() for t in (repo.get("topics") or [])}

    for kw in AI_ML_KEYWORDS:
        normalized = kw.replace("-", " ")
        if normalized in text or kw in topics:
            return True
    return False


def search_company_github(company_name: str, ticker: str) -> dict:
    """Search GitHub for a company's org and AI/ML repos.

    Returns dict with:
    - org_name, org_url
    - total_repos, ai_ml_repos count
    - total_stars, ai_ml_stars
    - recent_commits_30d (across AI repos)
    - top_ai_repos: list of top AI repo summaries
    """
    time.sleep(GITHUB_RATE_LIMIT_SECONDS)

    result = {
        "ticker": ticker,
        "company_name": company_name,
        "collected_at": datetime.utcnow().isoformat(),
        "org_name": None,
        "org_url": None,
        "total_repos": 0,
        "ai_ml_repos": 0,
        "total_stars": 0,
        "ai_ml_stars": 0,
        "recent_commits_30d": 0,
        "top_ai_repos": [],
    }

    # Step 1: Search for org
    clean_name = company_name.split(",")[0].split(" Inc")[0].split(" Corp")[0].strip()
    try:
        resp = requests.get(
            f"{GITHUB_API}/search/users",
            params={"q": f"{clean_name} type:org", "per_page": 5},
            headers=_headers(),
            timeout=15,
        )
        if resp.status_code != 200:
            logger.debug("GitHub org search returned %d for %s", resp.status_code, ticker)
            return result

        items = resp.json().get("items", [])
        if not items:
            return result

        org = items[0]
        result["org_name"] = org["login"]
        result["org_url"] = org["html_url"]

    except Exception as e:
        logger.warning("GitHub org search failed for %s: %s", ticker, e)
        return result

    time.sleep(GITHUB_RATE_LIMIT_SECONDS)

    # Step 2: List repos
    try:
        all_repos = []
        page = 1
        while page <= 3:  # Cap at 300 repos
            resp = requests.get(
                f"{GITHUB_API}/orgs/{result['org_name']}/repos",
                params={"sort": "updated", "per_page": 100, "page": page},
                headers=_headers(),
                timeout=15,
            )
            if resp.status_code != 200:
                break
            repos = resp.json()
            if not repos:
                break
            all_repos.extend(repos)
            if len(repos) < 100:
                break
            page += 1
            time.sleep(GITHUB_RATE_LIMIT_SECONDS)

        result["total_repos"] = len(all_repos)
        result["total_stars"] = sum(r.get("stargazers_count", 0) for r in all_repos)

        # Filter AI/ML repos
        ai_repos = [r for r in all_repos if _is_ai_repo(r)]
        result["ai_ml_repos"] = len(ai_repos)
        result["ai_ml_stars"] = sum(r.get("stargazers_count", 0) for r in ai_repos)

        # Count recent commits across top AI repos
        cutoff = (datetime.utcnow() - timedelta(days=30)).isoformat() + "Z"
        recent_count = 0
        for repo in ai_repos[:5]:  # Check top 5 AI repos for recent activity
            updated = repo.get("pushed_at", "")
            if updated and updated >= cutoff:
                recent_count += 1

        result["recent_commits_30d"] = recent_count

        # Top AI repos summary
        ai_repos_sorted = sorted(ai_repos, key=lambda r: r.get("stargazers_count", 0), reverse=True)
        result["top_ai_repos"] = [
            {
                "name": r["name"],
                "description": (r.get("description") or "")[:200],
                "stars": r.get("stargazers_count", 0),
                "language": r.get("language"),
                "updated_at": r.get("pushed_at"),
                "topics": r.get("topics", []),
            }
            for r in ai_repos_sorted[:10]
        ]

    except Exception as e:
        logger.warning("GitHub repo listing failed for %s: %s", ticker, e)

    return result
