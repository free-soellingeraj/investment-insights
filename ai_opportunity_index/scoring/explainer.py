"""Scoring explanation engine.

Re-derives scoring logic for a single company, capturing intermediate data
and source citations instead of just final scores.
"""

import logging
import math

import pandas as pd

from ai_opportunity_index.config import (
    OPPORTUNITY_WEIGHTS,
    RAW_DIR,
    REALIZATION_WEIGHTS,
)
from ai_opportunity_index.data.industry_mappings import (
    load_ai_applicability_scores,
    naics_to_soc_groups,
    sic_to_naics,
    sic_to_soc_groups,
)
from ai_opportunity_index.scoring.ai_opportunity import (
    B2B_NAICS,
    CUSTOMER_INDUSTRY_MAP,
    SECTOR_TO_AI_PROFILE,
    _find_score_column,
    _find_soc_column,
    _normalize_score,
)
from ai_opportunity_index.scoring.ai_realization import flag_discrepancies

logger = logging.getLogger(__name__)

# SOC group names for display
SOC_GROUP_NAMES = {
    "11-0000": "Management",
    "13-0000": "Business and Financial Operations",
    "15-0000": "Computer and Mathematical",
    "17-0000": "Architecture and Engineering",
    "19-0000": "Life, Physical, and Social Science",
    "21-0000": "Community and Social Service",
    "23-0000": "Legal",
    "25-0000": "Educational Instruction and Library",
    "27-0000": "Arts, Design, Entertainment, Sports, Media",
    "29-0000": "Healthcare Practitioners and Technical",
    "31-0000": "Healthcare Support",
    "33-0000": "Protective Service",
    "35-0000": "Food Preparation and Serving",
    "37-0000": "Building and Grounds Cleaning/Maintenance",
    "39-0000": "Personal Care and Service",
    "41-0000": "Sales and Related",
    "43-0000": "Office and Administrative Support",
    "45-0000": "Farming, Fishing, and Forestry",
    "47-0000": "Construction and Extraction",
    "49-0000": "Installation, Maintenance, and Repair",
    "51-0000": "Production",
    "53-0000": "Transportation and Material Moving",
}

# NAICS sector names for display
NAICS_NAMES = {
    "11": "Agriculture/Forestry",
    "21": "Mining",
    "22": "Utilities",
    "23": "Construction",
    "31": "Manufacturing (Food/Textile/Apparel)",
    "32": "Manufacturing (Chemical/Paper/Plastics)",
    "33": "Manufacturing (Metals/Machinery/Electronics)",
    "42": "Wholesale Trade",
    "44": "Retail Trade",
    "45": "Retail Trade",
    "48": "Transportation",
    "49": "Transportation/Warehousing",
    "51": "Information/Technology",
    "52": "Finance and Insurance",
    "53": "Real Estate",
    "54": "Professional/Technical Services",
    "55": "Management of Companies",
    "56": "Administrative/Waste Services",
    "61": "Education",
    "62": "Healthcare/Social Assistance",
    "71": "Arts/Entertainment/Recreation",
    "72": "Accommodation/Food Services",
    "81": "Other Services",
    "92": "Public Administration",
}


def _build_soc_lookup() -> dict[str, float]:
    """Build SOC group code -> AI applicability score lookup."""
    ai_scores_df = load_ai_applicability_scores()
    score_col = _find_score_column(ai_scores_df)
    soc_col = _find_soc_column(ai_scores_df)
    ai_scores_df["soc_major"] = ai_scores_df[soc_col].astype(str).str[:2] + "-0000"
    return ai_scores_df.groupby("soc_major")[score_col].mean().to_dict()


def _soc_detail(soc_code: str, soc_lookup: dict) -> dict:
    """Return display info for a SOC group."""
    return {
        "code": soc_code,
        "name": SOC_GROUP_NAMES.get(soc_code, soc_code),
        "ai_applicability": round(soc_lookup.get(soc_code, 0.15), 4),
    }


def explain_opportunity(
    sic: int | str | None,
    revenue: float | None,
    employees: int | None,
    sector: str | None,
    industry: str | None,
) -> dict:
    """Build a structured explanation of the AI Opportunity score."""
    soc_lookup = _build_soc_lookup()
    explanation = {
        "revenue": {},
        "cost": {},
        "composite": {},
        "sources": [],
    }

    used_sector_fallback = False
    naics = None
    soc_groups = []

    if not pd.isna(sic) and sic is not None:
        naics = sic_to_naics(sic)
        soc_groups = sic_to_soc_groups(sic)
    elif sector:
        used_sector_fallback = True

    # --- Revenue opportunity explanation ---
    rev_explanation = {
        "method": "sector_fallback" if used_sector_fallback else "sic_based",
        "sic": str(sic) if sic and not pd.isna(sic) else None,
        "naics": naics,
        "naics_name": NAICS_NAMES.get(naics, None) if naics else None,
        "sector": sector,
        "industry": industry,
    }

    if used_sector_fallback:
        profile = SECTOR_TO_AI_PROFILE.get(sector, {"soc_groups": [], "is_b2b": False, "base_score": 0.15})
        is_b2b = profile["is_b2b"]
        base_score = profile["base_score"]
        if is_b2b:
            rev_raw = base_score * 1.3
        else:
            rev_raw = base_score * 0.8

        # Industry-specific boosts
        industry_boost = False
        if industry:
            industry_lower = industry.lower()
            ai_industry_keywords = ["software", "cloud", "data", "ai", "semiconductor", "internet"]
            if any(kw in industry_lower for kw in ai_industry_keywords):
                rev_raw = min(0.38, rev_raw * 1.4)
                industry_boost = True

        rev_score = _normalize_score(rev_raw, 0.03, 0.38)
        rev_explanation.update({
            "is_b2b": is_b2b,
            "base_score": base_score,
            "b2b_multiplier": 1.3 if is_b2b else 0.8,
            "industry_boost_applied": industry_boost,
            "raw_score": round(rev_raw, 4),
            "normalized_score": round(rev_score, 4),
            "workforce_soc_groups": [_soc_detail(g, soc_lookup) for g in profile["soc_groups"]],
        })
    else:
        # SIC-based
        customer_naics_list = CUSTOMER_INDUSTRY_MAP.get(naics, []) if naics else []
        is_b2b = naics in B2B_NAICS if naics else False

        if customer_naics_list:
            customer_details = []
            customer_scores = []
            for cust_naics in customer_naics_list:
                cust_soc_groups = naics_to_soc_groups(cust_naics)
                cust_soc_scores = [soc_lookup.get(soc, 0.15) for soc in cust_soc_groups]
                avg_cust = sum(cust_soc_scores) / len(cust_soc_scores) if cust_soc_scores else 0.15
                customer_scores.extend(cust_soc_scores)
                customer_details.append({
                    "naics": cust_naics,
                    "naics_name": NAICS_NAMES.get(cust_naics, cust_naics),
                    "soc_groups": [_soc_detail(g, soc_lookup) for g in cust_soc_groups],
                    "avg_ai_applicability": round(avg_cust, 4),
                })

            avg = sum(customer_scores) / len(customer_scores) if customer_scores else 0.15
            b2b_boost = is_b2b
            if b2b_boost:
                avg_boosted = min(avg * 1.3, 0.38)
            else:
                avg_boosted = avg
            rev_score = _normalize_score(avg_boosted, 0.03, 0.38)

            rev_explanation.update({
                "is_b2b": is_b2b,
                "b2b_boost_applied": b2b_boost,
                "b2b_multiplier": 1.3 if b2b_boost else 1.0,
                "customer_industries": customer_details,
                "avg_customer_ai_applicability": round(avg, 4),
                "avg_after_boost": round(avg_boosted, 4),
                "normalized_score": round(rev_score, 4),
            })
        else:
            # Not a clear B2B industry
            if soc_groups:
                own_scores = [soc_lookup.get(g, 0.15) for g in soc_groups]
                avg = sum(own_scores) / len(own_scores)
                rev_score = _normalize_score(avg, 0.03, 0.38)
                rev_explanation.update({
                    "is_b2b": False,
                    "own_workforce_soc_groups": [_soc_detail(g, soc_lookup) for g in soc_groups],
                    "avg_ai_applicability": round(avg, 4),
                    "normalized_score": round(rev_score, 4),
                })
            else:
                rev_score = 0.3
                rev_explanation["normalized_score"] = 0.3
                rev_explanation["note"] = "No SIC/NAICS mapping available; default score used"

    explanation["revenue"] = rev_explanation

    # --- Cost opportunity explanation ---
    cost_explanation = {
        "method": "sector_fallback" if used_sector_fallback else "sic_based",
    }

    if used_sector_fallback:
        profile = SECTOR_TO_AI_PROFILE.get(sector, {"soc_groups": [], "is_b2b": False, "base_score": 0.15})
        if profile["soc_groups"]:
            scores = [soc_lookup.get(g, 0.15) for g in profile["soc_groups"]]
            cost_raw = sum(scores) / len(scores)
        else:
            cost_raw = profile["base_score"]

        emp_factor = 1.0
        if employees and employees > 0:
            emp_factor = 0.5 + 0.5 * min(math.log10(max(employees, 1)) / 5, 1.0)
            cost_raw_scaled = cost_raw * emp_factor
        else:
            cost_raw_scaled = cost_raw

        cost_score = _normalize_score(cost_raw_scaled, 0.03, 0.38)
        cost_explanation.update({
            "workforce_soc_groups": [_soc_detail(g, soc_lookup) for g in profile["soc_groups"]],
            "avg_ai_applicability": round(cost_raw, 4),
            "employees": employees,
            "employee_scaling_factor": round(emp_factor, 4),
            "raw_score_after_scaling": round(cost_raw_scaled, 4),
            "normalized_score": round(cost_score, 4),
        })
    else:
        if soc_groups:
            scores = [soc_lookup.get(g, 0.15) for g in soc_groups]
            avg = sum(scores) / len(scores)
            emp_factor = 1.0
            if employees and employees > 0:
                emp_factor = 0.5 + 0.5 * min(math.log10(max(employees, 1)) / 5, 1.0)
                avg_scaled = avg * emp_factor
            else:
                avg_scaled = avg

            cost_score = _normalize_score(avg_scaled, 0.03, 0.38)
            cost_explanation.update({
                "workforce_soc_groups": [_soc_detail(g, soc_lookup) for g in soc_groups],
                "avg_ai_applicability": round(avg, 4),
                "employees": employees,
                "employee_scaling_factor": round(emp_factor, 4),
                "raw_score_after_scaling": round(avg_scaled, 4),
                "normalized_score": round(cost_score, 4),
            })
        else:
            cost_score = 0.3
            cost_explanation["normalized_score"] = 0.3
            cost_explanation["note"] = "No SIC/NAICS mapping available; default score used"

    explanation["cost"] = cost_explanation

    # --- Composite ---
    rev_score = explanation["revenue"].get("normalized_score", 0.3)
    cost_score = explanation["cost"].get("normalized_score", 0.3)
    composite = (
        OPPORTUNITY_WEIGHTS["revenue_opportunity"] * rev_score
        + OPPORTUNITY_WEIGHTS["cost_opportunity"] * cost_score
    )
    explanation["composite"] = {
        "revenue_weight": OPPORTUNITY_WEIGHTS["revenue_opportunity"],
        "cost_weight": OPPORTUNITY_WEIGHTS["cost_opportunity"],
        "revenue_score": round(rev_score, 4),
        "cost_score": round(cost_score, 4),
        "composite_score": round(composite, 4),
    }

    # --- Sources ---
    explanation["sources"] = [
        {
            "name": 'Microsoft Research "Working with AI"',
            "detail": "AI applicability scores by occupation (arXiv:2507.07935)",
        },
        {
            "name": "BLS Occupation Data",
            "detail": "SOC major group classifications",
        },
    ]
    if sic and not pd.isna(sic):
        explanation["sources"].append({
            "name": "SEC EDGAR",
            "detail": f"SIC code {sic} from company filing",
        })
    if sector:
        explanation["sources"].append({
            "name": "Yahoo Finance",
            "detail": f"Sector: {sector}, Industry: {industry or 'N/A'}",
        })
    if employees:
        explanation["sources"].append({
            "name": "Yahoo Finance",
            "detail": f"Employee count: {employees:,}",
        })

    return explanation


def explain_product_analysis(company_name: str, ticker: str) -> dict:
    """Build explanation stub for product analysis sub-score."""
    # Product analysis requires API calls; return what we can from cached scores
    return {
        "available": False,
        "reason": "Product analysis requires news API calls; check scored evidence",
        "source": {
            "name": "GNews API / SEC EDGAR RSS",
            "detail": "News and press release analysis",
        },
    }


def explain_company(ticker: str) -> dict | None:
    """Build a full structured explanation for a company.

    Returns a dict with opportunity_explanation, realization evidence,
    discrepancy flags, and source citations. Returns None if company not found.
    """
    from ai_opportunity_index.storage.db import get_company_detail, get_evidence

    detail = get_company_detail(ticker)
    if not detail:
        return None

    # Get any persisted evidence
    evidence = get_evidence(ticker)

    # Build opportunity explanation
    opp_explanation = explain_opportunity(
        sic=detail.get("sic"),
        revenue=detail.get("revenue"),
        employees=detail.get("employees"),
        sector=detail.get("sector"),
        industry=detail.get("industry"),
    )

    # Build realization explanations
    filing_explanation = evidence.get("filing_nlp_evidence")
    if not filing_explanation:
        filing_explanation = {"available": False, "reason": "Check scored evidence items"}

    product_explanation = evidence.get("product_evidence", explain_product_analysis(
        detail.get("company_name", ticker), ticker
    ))

    # Realization composite explanation
    real_scores = detail.get("realization", {})
    available_scores = {}
    available_weights = {}
    weight_map = {
        "filing_nlp": ("filing_nlp", REALIZATION_WEIGHTS.get("filing_nlp", 0.40)),
        "product": ("product_analysis", REALIZATION_WEIGHTS.get("product_analysis", 0.30)),
        "web": ("web_enrichment", REALIZATION_WEIGHTS.get("web_enrichment", 0.30)),
    }

    for key, (weight_key, weight) in weight_map.items():
        score = real_scores.get(key)
        if score is not None:
            available_scores[key] = score
            available_weights[key] = weight

    total_weight = sum(available_weights.values())
    realization_composite_explanation = {
        "weights": {k: round(v, 4) for k, v in available_weights.items()},
        "scores": {k: round(v, 4) for k, v in available_scores.items()},
        "total_weight": round(total_weight, 4),
        "composite": real_scores.get("composite"),
    }

    # Discrepancy flags
    flags = flag_discrepancies({
        "filing_nlp_score": real_scores.get("filing_nlp"),
        "product_score": real_scores.get("product"),
    })

    return {
        "company": detail,
        "opportunity_explanation": opp_explanation,
        "realization_explanations": {
            "filing_nlp": filing_explanation,
            "product_analysis": product_explanation,
            "composite": realization_composite_explanation,
        },
        "flags": flags,
    }
