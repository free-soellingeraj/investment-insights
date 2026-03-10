"""Confidence calibration for LLM-generated estimates.

LLMs are notoriously poorly calibrated -- they say "0.8 confidence" but
are right only ~60% of the time. This module provides calibration functions
to map raw LLM confidence to empirically-calibrated probabilities.

Also provides dollar estimate sanity checking against market caps and revenue.
"""

from __future__ import annotations

import math

# ── Calibration curve parameters ──────────────────────────────────────────

# Floor and ceiling: nothing is ever certain or impossible.
CONFIDENCE_FLOOR = 0.05
CONFIDENCE_CEILING = 0.95

# Source-specific calibration parameters.
# Each entry is (transform_type, *params).
#   "platt":   sigmoid(a * x + b)
#   "linear":  clamp(x * scale)
_CALIBRATION_CURVES: dict[str, tuple[str, ...]] = {
    "llm": ("platt", "2.0", "-1.0"),       # sigmoid(2*x - 1): pulls extremes toward center
    "sec_filing": ("linear", "1.1"),        # SEC filings are high quality
    "news": ("linear", "0.8"),              # news is noisy
    "analyst": ("linear", "0.9"),           # decent but biased
    "github": ("linear", "0.7"),            # weak signal
}

# ── Source authority tiers ────────────────────────────────────────────────

_SOURCE_AUTHORITY: dict[str, float] = {
    "sec_filing_cik": 1.0,        # SEC filing with specific CIK
    "analyst_known_firm": 0.9,    # analyst report from known firm
    "earnings_call": 0.85,        # company earnings call
    "news_major": 0.7,            # major news outlet
    "blog": 0.4,                  # blog post
}
_DEFAULT_AUTHORITY = 0.5

# ── Temporal decay half-lives (days) ─────────────────────────────────────

_HALF_LIVES: dict[str, float] = {
    "sec_filing": 365.0,
    "earnings_call": 90.0,
    "news": 30.0,
    "analyst": 180.0,
    "github": 60.0,
}
_DEFAULT_HALF_LIFE = 90.0

# ── Dollar sanity thresholds ─────────────────────────────────────────────

_MAX_REVENUE_MULTIPLE = 0.5
_MAX_MARKET_CAP_FRACTION = 0.5
_GLOBAL_DOLLAR_CAP = 10_000_000_000.0  # $10B


def _sigmoid(x: float) -> float:
    """Numerically stable sigmoid."""
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    else:
        z = math.exp(x)
        return z / (1.0 + z)


def _clamp_confidence(value: float) -> float:
    """Clamp to [CONFIDENCE_FLOOR, CONFIDENCE_CEILING]."""
    return max(CONFIDENCE_FLOOR, min(CONFIDENCE_CEILING, value))


# ── Public API ────────────────────────────────────────────────────────────


def calibrate_confidence(raw_confidence: float, source_type: str = "llm") -> float:
    """Map raw LLM confidence to a calibrated probability.

    Applies a source-type-specific transformation, then clamps to
    [0.05, 0.95] -- nothing is ever certain.

    Parameters
    ----------
    raw_confidence : float
        Raw confidence value in [0, 1] from the LLM or source.
    source_type : str
        One of "llm", "sec_filing", "news", "analyst", "github".
        Unknown types fall back to the "llm" curve.

    Returns
    -------
    float
        Calibrated confidence in [0.05, 0.95].
    """
    curve = _CALIBRATION_CURVES.get(source_type, _CALIBRATION_CURVES["llm"])
    transform = curve[0]

    if transform == "platt":
        a = float(curve[1])
        b = float(curve[2])
        calibrated = _sigmoid(a * raw_confidence + b)
    elif transform == "linear":
        scale = float(curve[1])
        calibrated = raw_confidence * scale
    else:
        calibrated = raw_confidence

    return _clamp_confidence(calibrated)


def check_dollar_sanity(
    dollar_estimate: float,
    company_revenue: float | None,
    company_market_cap: float | None,
) -> tuple[float, list[str]]:
    """Check and adjust a dollar estimate for reasonableness.

    Returns the (possibly adjusted) estimate and a list of warning
    strings explaining any adjustments or flags.

    Parameters
    ----------
    dollar_estimate : float
        Raw dollar estimate from the LLM.
    company_revenue : float | None
        Company annual revenue, if known.
    company_market_cap : float | None
        Company market capitalisation, if known.

    Returns
    -------
    tuple[float, list[str]]
        (adjusted_estimate, warnings)
    """
    warnings: list[str] = []
    adjusted = dollar_estimate

    # Floor at $0
    if adjusted < 0:
        warnings.append(f"Negative estimate ${adjusted:,.0f} floored to $0")
        adjusted = 0.0

    # Check for suspiciously round numbers (likely a guess)
    if adjusted > 0 and adjusted == float(int(adjusted)):
        s = str(int(adjusted))
        # Round if it's all trailing zeros after the leading digit(s)
        significant = s.rstrip("0")
        trailing_zeros = len(s) - len(significant)
        if trailing_zeros >= 6 and adjusted >= 1_000_000:
            warnings.append(
                f"Estimate ${adjusted:,.0f} is suspiciously round -- likely a guess"
            )

    # Cap at 2x company revenue
    if company_revenue is not None and company_revenue > 0 and adjusted > 0:
        cap = company_revenue * _MAX_REVENUE_MULTIPLE
        if adjusted > cap:
            warnings.append(
                f"Estimate ${adjusted:,.0f} exceeds {_MAX_REVENUE_MULTIPLE}x "
                f"revenue (${company_revenue:,.0f}); capped to ${cap:,.0f}"
            )
            adjusted = cap

    # Cap at 0.5x market cap
    if company_market_cap is not None and company_market_cap > 0 and adjusted > 0:
        cap = company_market_cap * _MAX_MARKET_CAP_FRACTION
        if adjusted > cap:
            warnings.append(
                f"Estimate ${adjusted:,.0f} exceeds {_MAX_MARKET_CAP_FRACTION}x "
                f"market cap (${company_market_cap:,.0f}); capped to ${cap:,.0f}"
            )
            adjusted = cap

    # Global sanity cap
    if adjusted > _GLOBAL_DOLLAR_CAP:
        warnings.append(
            f"Estimate ${adjusted:,.0f} exceeds global cap of "
            f"${_GLOBAL_DOLLAR_CAP:,.0f}; capped"
        )
        adjusted = _GLOBAL_DOLLAR_CAP

    return (adjusted, warnings)


def source_authority_weight(
    source_type: str, source_authority: str | None
) -> float:
    """Return a credibility weight for the given source.

    Combines source_type and source_authority into a lookup key. Falls
    back to a default weight of 0.5 for unknown combinations.

    Parameters
    ----------
    source_type : str
        Category like "sec_filing", "news", "analyst", "github", etc.
    source_authority : str | None
        Optional qualifier such as a CIK number, firm name, or outlet tier.

    Returns
    -------
    float
        Authority weight in (0, 1].
    """
    # Build candidate keys from most specific to least
    if source_authority:
        authority_lower = source_authority.strip().lower()

        # SEC filing with specific CIK
        if source_type == "sec_filing" and authority_lower:
            return _SOURCE_AUTHORITY["sec_filing_cik"]

        # Analyst from a known firm
        if source_type == "analyst" and authority_lower:
            return _SOURCE_AUTHORITY["analyst_known_firm"]

        # Earnings call
        if authority_lower in ("earnings_call", "earnings call", "earnings"):
            return _SOURCE_AUTHORITY["earnings_call"]

        # News from major outlet
        if source_type == "news" and authority_lower in (
            "reuters", "bloomberg", "wsj", "wall street journal",
            "financial times", "ft", "nytimes", "new york times",
            "cnbc", "associated press", "ap",
        ):
            return _SOURCE_AUTHORITY["news_major"]

        # Blog
        if authority_lower in ("blog", "blog post", "medium", "substack"):
            return _SOURCE_AUTHORITY["blog"]

    # Type-level defaults for unqualified sources
    type_defaults: dict[str, float] = {
        "sec_filing": 0.9,
        "analyst": 0.7,
        "news": 0.6,
        "github": 0.5,
        "blog": 0.4,
    }
    return type_defaults.get(source_type, _DEFAULT_AUTHORITY)


def temporal_weight(days_old: int, source_type: str) -> float:
    """Compute an exponential-decay weight based on evidence age.

    Uses source-specific half-lives so that fast-moving signals (news)
    decay quickly while durable signals (SEC filings) persist.

    Parameters
    ----------
    days_old : int
        Age of the evidence in days.  Negative values are treated as 0.
    source_type : str
        Source category; determines the half-life.

    Returns
    -------
    float
        Weight in (0, 1].  Returns 1.0 for days_old <= 0.
    """
    if days_old <= 0:
        return 1.0

    half_life = _HALF_LIVES.get(source_type, _DEFAULT_HALF_LIFE)
    # Exponential decay: 0.5^(days / half_life)
    return math.pow(0.5, days_old / half_life)
