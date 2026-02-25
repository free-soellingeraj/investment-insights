"""Global configuration for the AI Opportunity Index.

All scoring weights, thresholds, and tuning parameters are defined here.
No magic constants should appear in scoring modules — reference this file.
"""

import os
from dataclasses import dataclass
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
MS_AI_DIR = DATA_DIR / "microsoft_ai_applicability"

# ── Database ───────────────────────────────────────────────────────────────
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://localhost:5432/ai_opportunity_index",
)
DB_POOL_SIZE = int(os.environ.get("DB_POOL_SIZE", "5"))
DB_MAX_OVERFLOW = int(os.environ.get("DB_MAX_OVERFLOW", "10"))

# ── SEC EDGAR ──────────────────────────────────────────────────────────────
SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_COMPANY_TICKERS_EXCHANGE_URL = (
    "https://www.sec.gov/files/company_tickers_exchange.json"
)
SEC_USER_AGENT = "AIOpportunityIndex research@example.com"
SEC_RATE_LIMIT_SECONDS = 0.15  # SEC asks for max 10 req/sec

# ── Microsoft AI Applicability ─────────────────────────────────────────────
MS_AI_GITHUB_RAW = (
    "https://raw.githubusercontent.com/microsoft/working-with-ai/main/data"
)
MS_AI_SCORES_FILENAME = "ai_applicability_scores.csv"

# ── BLS Data ───────────────────────────────────────────────────────────────
BLS_INDUSTRY_OCCUPATION_URL = (
    "https://www.bls.gov/emp/ind-occ-matrix/occupation-by-industry.xlsx"
)
BLS_NAICS_SOC_CROSSWALK_URL = (
    "https://www.bls.gov/soc/2018/soc_2018_direct_match_title_file.csv"
)

# ── Yahoo Finance ──────────────────────────────────────────────────────────
YF_RATE_LIMIT_SECONDS = 0.2

# ── GitHub ────────────────────────────────────────────────────────────────
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_RATE_LIMIT_SECONDS = 1.0

# ── Web Scrape Enrichment ─────────────────────────────────────────
WEB_SCRAPE_RATE_LIMIT_SECONDS = 1.0  # Polite scraping delay

# ── LLM Config ─────────────────────────────────────────────────────────────
LLM_PROVIDER = "anthropic"  # "anthropic" or "openai"
LLM_MODEL = "claude-sonnet-4-20250514"
LLM_MAX_TOKENS = 2048

# ══════════════════════════════════════════════════════════════════════════
# SCORING WEIGHTS & PARAMETERS
# All scoring tuning parameters live below. Modules import from here.
# ══════════════════════════════════════════════════════════════════════════

# ── Dimension Weights ─────────────────────────────────────────────────────

# How to weight cost vs revenue in the composite opportunity score
OPPORTUNITY_WEIGHTS = {
    "revenue_opportunity": 0.5,
    "cost_opportunity": 0.5,
}

# Sub-scorer weights in the capture/realization ensemble
CAPTURE_WEIGHTS = {
    "filing_nlp": 0.30,
    "product_analysis": 0.25,
    "web_enrichment": 0.20,
    "github": 0.15,
    "analyst": 0.10,
}
REALIZATION_WEIGHTS = CAPTURE_WEIGHTS  # backward compat alias

# Weights for combining cost and revenue ROI into a single number
ROI_WEIGHTS = {"cost": 0.5, "revenue": 0.5}

# Maximum ROI value (captures above this are capped)
ROI_CAP = 2.0

# Minimum denominator when computing ROI to avoid division by zero
ROI_MIN_DENOMINATOR = 0.01

# ── Quadrant Assignment ──────────────────────────────────────────────────

# Default thresholds for high/low classification. In bulk mode, medians
# are used instead.
QUADRANT_OPP_THRESHOLD = 0.5
QUADRANT_REAL_THRESHOLD = 0.5

QUADRANT_LABELS = {
    "high_opp_low_real": "Untapped Potential",
    "high_opp_high_real": "AI Leaders",
    "low_opp_high_real": "Over-investing?",
    "low_opp_low_real": "AI-Resistant",
}

# Legacy composite weights (cost/rev → single opportunity/realization number)
LEGACY_COMPOSITE_COST_WEIGHT = 0.5
LEGACY_COMPOSITE_REVENUE_WEIGHT = 0.5

# ── Signal Strength Thresholds ───────────────────────────────────────────
# Used to label evidence as "high", "medium", or "low" signal
SIGNAL_STRENGTH_HIGH = 0.6
SIGNAL_STRENGTH_MEDIUM = 0.3

# ── Discrepancy Flagging ────────────────────────────────────────────────
# Gap between scores that triggers a flag
DISCREPANCY_THRESHOLD = 0.3

# ── Product Analysis Sub-Scorer ──────────────────────────────────────────

# How many days back to search for company news
PRODUCT_NEWS_LOOKBACK_DAYS = 180

# ── Opportunity Scoring ──────────────────────────────────────────────────

# Default score when SIC/NAICS mapping is unavailable
OPP_DEFAULT_SCORE = 0.3

# Default SOC group AI applicability when not found in lookup
OPP_DEFAULT_SOC_SCORE = 0.15

# Normalization bounds for AI applicability → 0-1 score
# Range must cover post-boost values (B2B 1.3x, AI-industry 1.4x, employee scaling)
OPP_NORMALIZE_MIN = 0.03
OPP_NORMALIZE_MAX = 0.65

# B2B revenue boost multiplier
OPP_B2B_BOOST = 1.3

# Non-B2B revenue discount multiplier
OPP_NON_B2B_FACTOR = 0.8

# AI industry keyword boost multiplier
OPP_AI_INDUSTRY_BOOST = 1.4

# Employee scaling: emp_factor = BASE + SCALE * min(log10(employees) / DIVISOR, 1.0)
OPP_EMPLOYEE_SCALE_BASE = 0.5
OPP_EMPLOYEE_SCALE_FACTOR = 0.5
OPP_EMPLOYEE_SCALE_LOG_DIVISOR = 5

# Default sector fallback base_score
OPP_SECTOR_FALLBACK_BASE = 0.15

# ── Dollar Estimation ─────────────────────────────────────────────────
DOLLAR_PRODUCTIVITY_GAIN_PCT = 0.15      # conservative AI productivity improvement
DOLLAR_REVENUE_PENETRATION_RATE = 0.05   # share of revenue addressable by AI
DOLLAR_AVG_AI_SALARY = 135_000           # avg salary for AI/ML roles
DOLLAR_COST_PER_PATENT = 50_000          # avg cost per patent filing
DOLLAR_PRODUCT_REVENUE_BRACKETS = {      # revenue bracket → per-product dollar estimate
    1e9: 5_000_000,   # $1B+ company → $5M per AI product
    1e8: 500_000,      # $100M+ → $500K
    0: 50_000,         # smaller → $50K
}
# Horizon shape multipliers by capture stage
DOLLAR_HORIZON_REALIZED = (1.0, 1.0, 1.0)
DOLLAR_HORIZON_INVESTED = (0.33, 0.66, 1.0)
DOLLAR_HORIZON_PLANNED = (0.10, 0.40, 1.0)

# BLS salary data path
BLS_SALARY_DATA_PATH = DATA_DIR / "bls_salary_data.csv"

# ── External API Keys ─────────────────────────────────────────────────
PATENTSVIEW_API_KEY = os.environ.get("PATENTSVIEW_API_KEY", "")

# ── Link Discovery ────────────────────────────────────────────────────
LINK_DISCOVERY_MODEL = "gemini-2.5-flash"  # cheap LLM for URL classification
LINK_DISCOVERY_CONCURRENCY = 5  # parallel website fetches

# ── LLM Pipeline Config ──────────────────────────────────────────────
GOOGLE_VERTEX_PROJECT = os.environ.get("GOOGLE_VERTEX_PROJECT", "winona-quantitative-research")
GOOGLE_VERTEX_REGION = os.environ.get("GOOGLE_VERTEX_REGION", "us-central1")
LLM_PROVIDER_GOOGLE = "google-vertex"  # kept for backward compat; prefer get_google_provider()


def get_google_provider():
    """Return a GoogleProvider pinned to the correct Vertex AI project."""
    from pydantic_ai.providers.google import GoogleProvider

    return GoogleProvider(
        project=GOOGLE_VERTEX_PROJECT,
        location=GOOGLE_VERTEX_REGION,
        vertexai=True,
    )
LLM_EXTRACTION_MODEL = "gemini-2.5-flash"
LLM_ESTIMATION_MODEL = "gemini-2.5-flash"


# ── Cache Policies ────────────────────────────────────────────────────────


@dataclass(frozen=True)
class StageCachePolicy:
    """Per-stage cache retention policy.

    ttl_days: None = permanent cache; otherwise max age in days.
    cache_version: bump to invalidate all caches for this stage.
    check_type: "age" (default, TTL-based), "existence" (only check exists + version),
                or "none" (never auto-check, always run unless force).
    """
    ttl_days: int | None
    cache_version: str
    check_type: str = "age"


CACHE_POLICIES: dict[str, StageCachePolicy] = {
    "discover_links":         StageCachePolicy(ttl_days=365,  cache_version="v1"),
    "collect_news":           StageCachePolicy(ttl_days=7,    cache_version="v1"),
    "collect_github":         StageCachePolicy(ttl_days=7,    cache_version="v1"),
    "collect_analysts":       StageCachePolicy(ttl_days=7,    cache_version="v1"),
    "collect_web_enrichment": StageCachePolicy(ttl_days=7,    cache_version="v1"),
    "extract_filings":        StageCachePolicy(ttl_days=None, cache_version="v2", check_type="existence"),
    "extract_news":           StageCachePolicy(ttl_days=7,    cache_version="v2"),
    "value_evidence":         StageCachePolicy(ttl_days=None, cache_version="v1", check_type="none"),
    "score":                  StageCachePolicy(ttl_days=None, cache_version="v1", check_type="none"),
}
