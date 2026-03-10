"""Dimension 1: AI Opportunity Score.

Maps each company's industry to an AI applicability score using:
- Revenue opportunity: customers' AI applicability (can the company sell AI solutions?)
- Cost opportunity: own workforce AI applicability (can the company save costs with AI?)
"""

import logging

import pandas as pd

from ai_opportunity_index.config import (
    OPPORTUNITY_WEIGHTS,
    OPP_AI_INDUSTRY_BOOST,
    OPP_B2B_BOOST,
    OPP_DEFAULT_SCORE,
    OPP_DEFAULT_SOC_SCORE,
    OPP_EMPLOYEE_SCALE_BASE,
    OPP_EMPLOYEE_SCALE_FACTOR,
    OPP_EMPLOYEE_SCALE_LOG_DIVISOR,
    OPP_NON_B2B_FACTOR,
    OPP_NORMALIZE_MAX,
    OPP_NORMALIZE_MIN,
    OPP_SECTOR_FALLBACK_BASE,
)
from ai_opportunity_index.data.industry_mappings import (
    build_industry_occupation_matrix,
    load_ai_applicability_scores,
    sic_to_naics,
    sic_to_soc_groups,
)

logger = logging.getLogger(__name__)

# Yahoo Finance sector → approximate SOC occupation groups with AI applicability
SECTOR_TO_AI_PROFILE = {
    "Technology": {"soc_groups": ["15-0000", "17-0000", "27-0000"], "is_b2b": True, "base_score": 0.32},
    "Communication Services": {"soc_groups": ["27-0000", "15-0000", "41-0000"], "is_b2b": True, "base_score": 0.35},
    "Financial Services": {"soc_groups": ["13-0000", "43-0000"], "is_b2b": False, "base_score": 0.30},
    "Healthcare": {"soc_groups": ["29-0000", "31-0000", "19-0000"], "is_b2b": False, "base_score": 0.18},
    "Consumer Cyclical": {"soc_groups": ["41-0000", "35-0000"], "is_b2b": False, "base_score": 0.22},
    "Consumer Defensive": {"soc_groups": ["41-0000", "51-0000"], "is_b2b": False, "base_score": 0.18},
    "Industrials": {"soc_groups": ["51-0000", "47-0000", "17-0000"], "is_b2b": False, "base_score": 0.15},
    "Energy": {"soc_groups": ["47-0000", "51-0000"], "is_b2b": False, "base_score": 0.10},
    "Basic Materials": {"soc_groups": ["51-0000", "47-0000"], "is_b2b": False, "base_score": 0.09},
    "Real Estate": {"soc_groups": ["41-0000", "43-0000", "37-0000"], "is_b2b": False, "base_score": 0.20},
    "Utilities": {"soc_groups": ["51-0000", "47-0000"], "is_b2b": False, "base_score": 0.10},
}

# Industries whose customers are other businesses (B2B).
# These can potentially sell AI to their customers.
B2B_NAICS = {"51", "54", "55", "56"}

# Map NAICS to typical customer industry NAICS codes
CUSTOMER_INDUSTRY_MAP = {
    "51": ["52", "54", "62", "44", "72"],  # Info Tech serves: Finance, Professional, Health, Retail, Food
    "54": ["52", "51", "33", "62"],  # Professional serves: Finance, Info, Manufacturing, Health
    "33": ["44", "48", "23", "92"],  # Manufacturing serves: Retail, Transport, Construction, Gov
    "52": ["54", "31", "44", "72"],  # Finance serves: Professional, Manufacturing, Retail, Food
    "62": ["92", "61"],  # Healthcare serves: Government, Education
}


def score_revenue_opportunity(
    sic: int | str | None,
    revenue: float | None = None,
    employees: int | None = None,
) -> dict:
    """Score the revenue-side AI opportunity for a company.

    Higher score if the company's customers have high AI applicability
    (meaning the company could sell AI solutions to them).

    Returns dict with 'score' and 'detail' breakdown.
    """
    detail = {"method": "sic_based", "sic": str(sic) if sic else None}

    if pd.isna(sic):
        return {"score": OPP_DEFAULT_SCORE, "detail": {**detail, "method": "default", "reason": "No SIC code available"}}

    naics = sic_to_naics(sic)
    if naics is None:
        return {"score": OPP_DEFAULT_SCORE, "detail": {**detail, "method": "default", "reason": "SIC code could not be mapped to NAICS"}}

    detail["naics"] = naics

    ai_scores_df = load_ai_applicability_scores()

    # Build SOC score lookup + name lookup
    score_col = _find_score_column(ai_scores_df)
    soc_col = _find_soc_column(ai_scores_df)
    name_col = _find_name_column(ai_scores_df)
    ai_scores_df["soc_major"] = ai_scores_df[soc_col].astype(str).str[:2] + "-0000"
    soc_lookup = ai_scores_df.groupby("soc_major")[score_col].mean().to_dict()
    soc_names = {}
    if name_col:
        soc_names = ai_scores_df.groupby("soc_major")[name_col].first().to_dict()

    # Get customer industries and their occupation AI applicability
    customer_naics_list = CUSTOMER_INDUSTRY_MAP.get(naics, [])
    detail["is_b2b"] = naics in B2B_NAICS

    if not customer_naics_list:
        # Not a clear B2B industry — moderate revenue opportunity from own products
        own_soc_groups = sic_to_soc_groups(sic)
        if own_soc_groups:
            own_scores = [soc_lookup.get(g, OPP_DEFAULT_SOC_SCORE) for g in own_soc_groups]
            detail["method"] = "own_workforce"
            detail["reason"] = "Not a B2B industry — revenue opportunity based on own workforce AI applicability"
            detail["workforce_roles"] = [
                {"soc": g, "name": soc_names.get(g, g), "ai_applicability": round(soc_lookup.get(g, OPP_DEFAULT_SOC_SCORE), 4)}
                for g in own_soc_groups
            ]
            raw = min(sum(own_scores) / len(own_scores), OPP_NORMALIZE_MAX * 0.9)
            score = _normalize_score(raw, OPP_NORMALIZE_MIN, OPP_NORMALIZE_MAX)
            return {"score": score, "detail": detail}
        return {"score": OPP_DEFAULT_SCORE, "detail": {**detail, "reason": "No occupational mapping available"}}

    # Average AI applicability across customer industries
    customer_detail = []
    customer_scores = []
    for cust_naics in customer_naics_list:
        from ai_opportunity_index.data.industry_mappings import naics_to_soc_groups
        cust_soc_groups = naics_to_soc_groups(cust_naics)
        cust_name = _naics_name(cust_naics)
        cust_soc_scores = []
        roles = []
        for soc in cust_soc_groups:
            s = soc_lookup.get(soc, OPP_DEFAULT_SOC_SCORE)
            customer_scores.append(s)
            cust_soc_scores.append(s)
            roles.append({"soc": soc, "name": soc_names.get(soc, soc), "ai_applicability": round(s, 4)})
        if cust_soc_scores:
            customer_detail.append({
                "naics": cust_naics,
                "industry": cust_name,
                "avg_ai_applicability": round(sum(cust_soc_scores) / len(cust_soc_scores), 4),
                "roles": roles,
            })

    detail["customer_industries"] = customer_detail

    if customer_scores:
        avg = sum(customer_scores) / len(customer_scores)
        detail["raw_avg"] = round(avg, 4)
        if naics in B2B_NAICS:
            detail["b2b_boost"] = OPP_B2B_BOOST
            avg = avg * OPP_B2B_BOOST
            detail["after_boost"] = round(avg, 4)
        detail["reason"] = "Revenue opportunity based on customer industries' AI applicability"
        avg = min(avg, OPP_NORMALIZE_MAX * 0.9)
        score = _normalize_score(avg, OPP_NORMALIZE_MIN, OPP_NORMALIZE_MAX)
        return {"score": score, "detail": detail}

    return {"score": OPP_DEFAULT_SCORE, "detail": {**detail, "reason": "No customer industry data"}}


def score_cost_opportunity(
    sic: int | str | None,
    employees: int | None = None,
    revenue: float | None = None,
) -> dict:
    """Score the cost-side AI opportunity for a company.

    Higher score if the company's own workforce has high AI applicability
    (meaning internal AI deployment could reduce costs).

    Returns dict with 'score' and 'detail' breakdown.
    """
    detail = {"method": "sic_based", "sic": str(sic) if sic else None}

    if pd.isna(sic):
        return {"score": OPP_DEFAULT_SCORE, "detail": {**detail, "method": "default", "reason": "No SIC code available"}}

    soc_groups = sic_to_soc_groups(sic)
    if not soc_groups:
        return {"score": OPP_DEFAULT_SCORE, "detail": {**detail, "reason": "SIC code could not be mapped to occupations"}}

    naics = sic_to_naics(sic)
    detail["naics"] = naics

    ai_scores_df = load_ai_applicability_scores()
    score_col = _find_score_column(ai_scores_df)
    soc_col = _find_soc_column(ai_scores_df)
    name_col = _find_name_column(ai_scores_df)
    ai_scores_df["soc_major"] = ai_scores_df[soc_col].astype(str).str[:2] + "-0000"
    soc_lookup = ai_scores_df.groupby("soc_major")[score_col].mean().to_dict()
    soc_names = {}
    if name_col:
        soc_names = ai_scores_df.groupby("soc_major")[name_col].first().to_dict()

    scores = [soc_lookup.get(g, OPP_DEFAULT_SOC_SCORE) for g in soc_groups]
    avg = sum(scores) / len(scores)

    # Build workforce breakdown
    workforce_roles = []
    for g, s in zip(soc_groups, scores):
        workforce_roles.append({
            "soc": g,
            "name": soc_names.get(g, g),
            "ai_applicability": round(s, 4),
        })
    detail["workforce_roles"] = workforce_roles
    detail["raw_avg"] = round(avg, 4)

    # Scale by employee count if available (more employees = more savings potential)
    if employees and employees > 0:
        import math
        emp_factor = OPP_EMPLOYEE_SCALE_BASE + OPP_EMPLOYEE_SCALE_FACTOR * min(math.log10(max(employees, 1)) / OPP_EMPLOYEE_SCALE_LOG_DIVISOR, 1.0)
        detail["employee_count"] = employees
        detail["employee_scaling_factor"] = round(emp_factor, 4)
        avg = avg * emp_factor
        detail["after_employee_scaling"] = round(avg, 4)

    detail["reason"] = "Cost opportunity based on workforce occupations' AI applicability"
    avg = min(avg, OPP_NORMALIZE_MAX * 0.9)
    score = _normalize_score(avg, OPP_NORMALIZE_MIN, OPP_NORMALIZE_MAX)
    return {"score": score, "detail": detail}


def compute_opportunity_score(
    sic: int | str | None,
    revenue: float | None = None,
    employees: int | None = None,
    sector: str | None = None,
    industry: str | None = None,
) -> dict:
    """Compute the composite AI Opportunity score.

    Uses SIC code if available, otherwise falls back to Yahoo Finance sector/industry.

    Returns dict with: revenue_opportunity, cost_opportunity, composite_opportunity,
    plus revenue_detail and cost_detail breakdowns.
    """
    # If SIC is missing but we have sector, use sector-based scoring
    if (pd.isna(sic) or sic is None) and sector:
        return _score_from_sector(sector, industry, revenue, employees)

    rev_result = score_revenue_opportunity(sic, revenue=revenue, employees=employees)
    cost_result = score_cost_opportunity(sic, employees=employees, revenue=revenue)

    rev_score = rev_result["score"]
    cost_score = cost_result["score"]

    composite = (
        OPPORTUNITY_WEIGHTS["revenue_opportunity"] * rev_score
        + OPPORTUNITY_WEIGHTS["cost_opportunity"] * cost_score
    )

    if composite > 0.95:
        logger.warning("Composite opportunity %.4f exceeds 0.95 for sic=%s", composite, sic)

    return {
        "revenue_opportunity": round(rev_score, 4),
        "cost_opportunity": round(cost_score, 4),
        "composite_opportunity": round(composite, 4),
        "revenue_detail": rev_result["detail"],
        "cost_detail": cost_result["detail"],
    }


def _score_from_sector(
    sector: str,
    industry: str | None,
    revenue: float | None,
    employees: int | None,
) -> dict:
    """Score opportunity using Yahoo Finance sector when SIC is unavailable."""
    profile = SECTOR_TO_AI_PROFILE.get(sector, {"soc_groups": [], "is_b2b": False, "base_score": OPP_SECTOR_FALLBACK_BASE})

    ai_scores_df = load_ai_applicability_scores()
    score_col = _find_score_column(ai_scores_df)
    soc_col = _find_soc_column(ai_scores_df)
    name_col = _find_name_column(ai_scores_df)
    ai_scores_df["soc_major"] = ai_scores_df[soc_col].astype(str).str[:2] + "-0000"
    soc_lookup = ai_scores_df.groupby("soc_major")[score_col].mean().to_dict()
    soc_names = {}
    if name_col:
        soc_names = ai_scores_df.groupby("soc_major")[name_col].first().to_dict()

    cost_detail = {"method": "sector_fallback", "sector": sector, "industry": industry}
    rev_detail = {"method": "sector_fallback", "sector": sector, "industry": industry, "is_b2b": profile["is_b2b"]}

    # Cost opportunity: AI applicability of workforce
    if profile["soc_groups"]:
        scores = [soc_lookup.get(g, OPP_DEFAULT_SOC_SCORE) for g in profile["soc_groups"]]
        cost_raw = sum(scores) / len(scores)
        cost_detail["workforce_roles"] = [
            {"soc": g, "name": soc_names.get(g, g), "ai_applicability": round(soc_lookup.get(g, OPP_DEFAULT_SOC_SCORE), 4)}
            for g in profile["soc_groups"]
        ]
    else:
        cost_raw = profile["base_score"]
    cost_detail["raw_avg"] = round(cost_raw, 4)

    # Scale by employee count
    if employees and employees > 0:
        import math
        emp_factor = OPP_EMPLOYEE_SCALE_BASE + OPP_EMPLOYEE_SCALE_FACTOR * min(math.log10(max(employees, 1)) / OPP_EMPLOYEE_SCALE_LOG_DIVISOR, 1.0)
        cost_raw = cost_raw * emp_factor
        cost_detail["employee_count"] = employees
        cost_detail["employee_scaling_factor"] = round(emp_factor, 4)

    cost_raw = cost_raw * 0.7
    cost_detail["sic_null_penalty"] = 0.7
    cost_detail["reason"] = f"Cost opportunity from {sector} sector workforce profile (SIC-null penalty applied)"
    cost_raw = min(cost_raw, OPP_NORMALIZE_MAX * 0.9)
    cost_score = _normalize_score(cost_raw, OPP_NORMALIZE_MIN, OPP_NORMALIZE_MAX)

    # Revenue opportunity
    if profile["is_b2b"]:
        rev_raw = profile["base_score"] * OPP_B2B_BOOST
        rev_detail["reason"] = f"B2B {sector} — customers have high AI applicability"
        rev_detail["b2b_boost"] = OPP_B2B_BOOST
    else:
        rev_raw = profile["base_score"] * OPP_NON_B2B_FACTOR
        rev_detail["reason"] = f"Non-B2B {sector} — revenue opportunity from own AI-enhanced products"

    # Industry-specific boosts
    if industry:
        industry_lower = industry.lower()
        ai_industry_keywords = ["software", "cloud", "data", "ai", "semiconductor", "internet"]
        if any(kw in industry_lower for kw in ai_industry_keywords):
            rev_raw = rev_raw * OPP_AI_INDUSTRY_BOOST
            rev_detail["ai_industry_boost"] = OPP_AI_INDUSTRY_BOOST
            rev_detail["reason"] += f" (boosted: '{industry}' is AI-adjacent)"

    rev_raw = rev_raw * 0.7
    rev_detail["sic_null_penalty"] = 0.7
    rev_raw = min(rev_raw, OPP_NORMALIZE_MAX * 0.9)
    rev_score = _normalize_score(rev_raw, OPP_NORMALIZE_MIN, OPP_NORMALIZE_MAX)

    composite = (
        OPPORTUNITY_WEIGHTS["revenue_opportunity"] * rev_score
        + OPPORTUNITY_WEIGHTS["cost_opportunity"] * cost_score
    )

    if composite > 0.95:
        logger.warning("Composite opportunity %.4f exceeds 0.95 for sector=%s industry=%s", composite, sector, industry)

    return {
        "revenue_opportunity": round(rev_score, 4),
        "cost_opportunity": round(cost_score, 4),
        "composite_opportunity": round(composite, 4),
        "revenue_detail": rev_detail,
        "cost_detail": cost_detail,
    }


def _normalize_score(value: float, min_val: float, max_val: float) -> float:
    """Normalize a value to 0-1 range."""
    if max_val == min_val:
        return 0.5
    return max(0.0, min(1.0, (value - min_val) / (max_val - min_val)))


def _find_score_column(df: pd.DataFrame) -> str:
    for c in ["ai_applicability_score", "score", "applicability"]:
        if c in df.columns:
            return c
    return df.columns[-1]


def _find_soc_column(df: pd.DataFrame) -> str:
    for c in ["soc_group", "soc_code", "soc", "occupation_code"]:
        if c in df.columns:
            return c
    return df.columns[0]


def _find_name_column(df: pd.DataFrame) -> str | None:
    for c in ["occupation_group_name", "occupation_name", "name", "title", "description"]:
        if c in df.columns:
            return c
    return None


# NAICS 2-digit sector names for human-readable output
_NAICS_NAMES = {
    "11": "Agriculture", "21": "Mining", "22": "Utilities", "23": "Construction",
    "31": "Manufacturing (Food/Textile)", "32": "Manufacturing (Chemical/Materials)",
    "33": "Manufacturing (Metal/Equipment)", "42": "Wholesale Trade",
    "44": "Retail Trade", "45": "Retail Trade", "48": "Transportation",
    "49": "Postal/Warehousing", "51": "Information / Technology",
    "52": "Finance & Insurance", "53": "Real Estate", "54": "Professional/Technical Services",
    "55": "Management of Companies", "56": "Administrative Services",
    "61": "Education", "62": "Healthcare & Social Services",
    "71": "Arts & Entertainment", "72": "Accommodation & Food",
    "81": "Other Services", "92": "Public Administration",
}


def _naics_name(code: str) -> str:
    return _NAICS_NAMES.get(code, f"NAICS {code}")
