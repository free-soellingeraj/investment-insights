"""Map SIC → NAICS → SOC occupations using BLS crosswalk data."""

import logging
from pathlib import Path

import pandas as pd
import requests

from ai_opportunity_index.config import (
    MS_AI_DIR,
    MS_AI_GITHUB_RAW,
    MS_AI_SCORES_FILENAME,
    PROCESSED_DIR,
    RAW_DIR,
    SEC_USER_AGENT,
)

logger = logging.getLogger(__name__)

# ── SIC → NAICS mapping (BLS/Census) ──────────────────────────────────────

# Condensed SIC-to-NAICS mapping for the most common 2-digit SIC ranges.
# Full mapping has ~1,000+ entries; this covers the structural backbone.
# A comprehensive mapping file can be downloaded from Census.gov.
SIC_TO_NAICS_2DIGIT = {
    1: "11",   # Agriculture → Agriculture
    2: "11",   # Agriculture → Agriculture
    7: "11",   # Agriculture services → Agriculture
    8: "21",   # Forestry → Agriculture/Mining
    9: "11",   # Fishing → Agriculture
    10: "21",  # Metal mining → Mining
    12: "21",  # Coal mining → Mining
    13: "21",  # Oil and gas → Mining
    14: "21",  # Nonmetallic minerals → Mining
    15: "23",  # Construction general → Construction
    16: "23",  # Heavy construction → Construction
    17: "23",  # Special trade contractors → Construction
    20: "31",  # Food products → Manufacturing
    21: "31",  # Tobacco → Manufacturing
    22: "31",  # Textile → Manufacturing
    23: "31",  # Apparel → Manufacturing
    24: "32",  # Lumber/Wood → Manufacturing
    25: "33",  # Furniture → Manufacturing
    26: "32",  # Paper → Manufacturing
    27: "51",  # Printing/Publishing → Information
    28: "32",  # Chemicals → Manufacturing
    29: "32",  # Petroleum refining → Manufacturing
    30: "32",  # Rubber/Plastics → Manufacturing
    31: "31",  # Leather → Manufacturing
    32: "32",  # Stone/Clay/Glass → Manufacturing
    33: "33",  # Primary metals → Manufacturing
    34: "33",  # Fabricated metals → Manufacturing
    35: "33",  # Industrial machinery → Manufacturing
    36: "33",  # Electronic equipment → Manufacturing
    37: "33",  # Transportation equipment → Manufacturing
    38: "33",  # Instruments → Manufacturing
    39: "33",  # Misc manufacturing → Manufacturing
    40: "48",  # Railroad transport → Transportation
    41: "48",  # Transit → Transportation
    42: "48",  # Trucking → Transportation
    43: "49",  # US Postal → Transportation/Utilities
    44: "48",  # Water transport → Transportation
    45: "48",  # Air transport → Transportation
    46: "48",  # Pipelines → Transportation
    47: "48",  # Transportation services → Transportation
    48: "51",  # Communications → Information
    49: "22",  # Utilities → Utilities
    50: "42",  # Wholesale durable → Wholesale
    51: "42",  # Wholesale nondurable → Wholesale
    52: "44",  # Retail building materials → Retail
    53: "44",  # General merchandise → Retail
    54: "44",  # Food stores → Retail
    55: "44",  # Auto dealers → Retail
    56: "44",  # Apparel stores → Retail
    57: "44",  # Furniture stores → Retail
    58: "72",  # Eating/Drinking → Accommodation/Food
    59: "44",  # Misc retail → Retail
    60: "52",  # Depository institutions → Finance
    61: "52",  # Nondepository credit → Finance
    62: "52",  # Security brokers → Finance
    63: "52",  # Insurance carriers → Finance
    64: "52",  # Insurance agents → Finance
    65: "53",  # Real estate → Real Estate
    67: "52",  # Holding/Investment → Finance
    70: "72",  # Hotels → Accommodation
    72: "81",  # Personal services → Other Services
    73: "54",  # Business services → Professional/Technical
    75: "81",  # Auto repair → Other Services
    76: "81",  # Misc repair → Other Services
    78: "71",  # Motion pictures → Arts/Entertainment
    79: "71",  # Amusement/Recreation → Arts/Entertainment
    80: "62",  # Health services → Health Care
    81: "54",  # Legal services → Professional/Technical
    82: "61",  # Educational services → Education
    83: "62",  # Social services → Health Care/Social
    84: "81",  # Museums → Other Services
    86: "81",  # Membership organizations → Other Services
    87: "54",  # Engineering/Management → Professional/Technical
    88: "81",  # Private households → Other Services
    89: "54",  # Misc services → Professional/Technical
    91: "92",  # Executive/Legislative → Public Admin
    92: "92",  # Justice/Public order → Public Admin
    93: "92",  # Public finance → Public Admin
    94: "92",  # Administration → Public Admin
    95: "92",  # Environmental quality → Public Admin
    96: "92",  # Admin economic programs → Public Admin
    97: "92",  # National security → Public Admin
    99: "92",  # Nonclassifiable → Public Admin
}

# Approximate NAICS 2-digit → major SOC occupation groups
# This maps industries to the occupation groups that predominantly work in them
NAICS_TO_SOC_GROUPS = {
    "11": ["45-0000"],  # Agriculture → Farming/Fishing
    "21": ["47-0000"],  # Mining → Construction/Extraction
    "22": ["51-0000"],  # Utilities → Production
    "23": ["47-0000"],  # Construction → Construction/Extraction
    "31": ["51-0000"],  # Manufacturing → Production
    "32": ["51-0000"],  # Manufacturing → Production
    "33": ["51-0000", "17-0000"],  # Manufacturing → Production + Engineering
    "42": ["41-0000", "43-0000"],  # Wholesale → Sales + Office
    "44": ["41-0000"],  # Retail → Sales
    "45": ["41-0000"],  # Retail → Sales
    "48": ["53-0000"],  # Transportation → Transportation
    "49": ["53-0000", "49-0000"],  # Transportation → Transport + Maintenance
    "51": ["15-0000", "27-0000"],  # Information → Computer + Media
    "52": ["13-0000", "43-0000"],  # Finance → Business/Financial + Office
    "53": ["41-0000", "43-0000"],  # Real Estate → Sales + Office
    "54": ["15-0000", "17-0000", "13-0000"],  # Professional → Computer + Engineering + Business
    "55": ["11-0000", "13-0000"],  # Management of Companies → Management + Business
    "56": ["37-0000", "43-0000"],  # Admin/Waste → Building Maintenance + Office
    "61": ["25-0000"],  # Education → Education
    "62": ["29-0000", "31-0000"],  # Health Care → Healthcare Practitioners + Support
    "71": ["27-0000"],  # Arts/Entertainment → Arts/Media
    "72": ["35-0000"],  # Accommodation/Food → Food Prep
    "81": ["49-0000"],  # Other Services → Maintenance/Repair
    "92": ["33-0000", "43-0000"],  # Public Admin → Protective Service + Office
}


def sic_to_naics(sic_code: int | str) -> str | None:
    """Convert a SIC code to its approximate 2-digit NAICS sector."""
    try:
        sic_2digit = int(str(sic_code)[:2])
    except (ValueError, TypeError):
        return None
    return SIC_TO_NAICS_2DIGIT.get(sic_2digit)


def naics_to_soc_groups(naics_2digit: str) -> list[str]:
    """Map a 2-digit NAICS code to major SOC occupation groups."""
    return NAICS_TO_SOC_GROUPS.get(naics_2digit, [])


def sic_to_soc_groups(sic_code: int | str) -> list[str]:
    """Map a SIC code through NAICS to SOC occupation groups."""
    naics = sic_to_naics(sic_code)
    if naics is None:
        return []
    return naics_to_soc_groups(naics)


def download_ms_ai_scores() -> pd.DataFrame:
    """Download Microsoft AI applicability scores from GitHub.

    Tries multiple possible file paths in the repo. If download fails,
    falls back to the occupation rankings embedded in local docs.
    """
    possible_paths = [
        f"{MS_AI_GITHUB_RAW}/{MS_AI_SCORES_FILENAME}",
        f"{MS_AI_GITHUB_RAW}/occupation_scores.csv",
        f"{MS_AI_GITHUB_RAW}/applicability_by_occupation.csv",
    ]

    for url in possible_paths:
        try:
            logger.info("Trying to download MS AI scores from %s", url)
            resp = requests.get(url, timeout=30)
            if resp.status_code == 200:
                out_path = MS_AI_DIR / MS_AI_SCORES_FILENAME
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_text(resp.text)
                df = pd.read_csv(out_path)
                logger.info("Downloaded MS AI scores: %d rows", len(df))
                return df
        except Exception as e:
            logger.debug("Failed to download from %s: %s", url, e)

    # Fall back to building from embedded knowledge
    logger.warning("Could not download MS AI scores; using built-in approximation")
    return _build_fallback_scores()


def _build_fallback_scores() -> pd.DataFrame:
    """Build approximate AI applicability scores from the Microsoft research.

    Based on the occupation rankings documented in the local markdown files.
    Scores range from 0.03 (Forest/Conservation) to 0.38 (Media/Communication).
    """
    # Major SOC groups with approximate scores from the Microsoft research
    data = [
        ("11-0000", "Management Occupations", 0.22),
        ("13-0000", "Business and Financial Operations", 0.28),
        ("15-0000", "Computer and Mathematical", 0.32),
        ("17-0000", "Architecture and Engineering", 0.20),
        ("19-0000", "Life, Physical, and Social Science", 0.22),
        ("21-0000", "Community and Social Service", 0.18),
        ("23-0000", "Legal Occupations", 0.25),
        ("25-0000", "Educational Instruction and Library", 0.31),
        ("27-0000", "Arts, Design, Entertainment, Sports, Media", 0.38),
        ("29-0000", "Healthcare Practitioners and Technical", 0.15),
        ("31-0000", "Healthcare Support", 0.10),
        ("33-0000", "Protective Service", 0.07),
        ("35-0000", "Food Preparation and Serving", 0.08),
        ("37-0000", "Building and Grounds Cleaning and Maintenance", 0.06),
        ("39-0000", "Personal Care and Service", 0.12),
        ("41-0000", "Sales and Related", 0.35),
        ("43-0000", "Office and Administrative Support", 0.33),
        ("45-0000", "Farming, Fishing, and Forestry", 0.03),
        ("47-0000", "Construction and Extraction", 0.07),
        ("49-0000", "Installation, Maintenance, and Repair", 0.10),
        ("51-0000", "Production Occupations", 0.09),
        ("53-0000", "Transportation and Material Moving", 0.07),
    ]
    df = pd.DataFrame(data, columns=["soc_group", "occupation_group_name", "ai_applicability_score"])

    out_path = MS_AI_DIR / "fallback_ai_scores.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)

    return df


def load_ai_applicability_scores() -> pd.DataFrame:
    """Load AI applicability scores, downloading if necessary."""
    # Check for downloaded file first
    cached_path = MS_AI_DIR / MS_AI_SCORES_FILENAME
    if cached_path.exists():
        return pd.read_csv(cached_path)

    fallback_path = MS_AI_DIR / "fallback_ai_scores.csv"
    if fallback_path.exists():
        return pd.read_csv(fallback_path)

    return download_ms_ai_scores()


def build_industry_occupation_matrix(universe_df: pd.DataFrame) -> pd.DataFrame:
    """Map each company in the universe to its SOC occupation groups and AI scores.

    Args:
        universe_df: DataFrame with at least 'ticker' and 'sic' columns.

    Returns:
        DataFrame with columns: ticker, sic, naics, soc_groups, avg_ai_applicability.
    """
    ai_scores = load_ai_applicability_scores()

    # Build a lookup from SOC group → score
    score_col = None
    for candidate in ["ai_applicability_score", "score", "applicability"]:
        if candidate in ai_scores.columns:
            score_col = candidate
            break
    if score_col is None:
        score_col = ai_scores.columns[-1]  # last column as fallback

    soc_col = None
    for candidate in ["soc_group", "soc_code", "soc", "occupation_code"]:
        if candidate in ai_scores.columns:
            soc_col = candidate
            break
    if soc_col is None:
        soc_col = ai_scores.columns[0]

    # Normalize SOC codes to major group level (XX-0000)
    ai_scores["soc_major"] = ai_scores[soc_col].astype(str).str[:2] + "-0000"
    soc_to_score = (
        ai_scores.groupby("soc_major")[score_col].mean().to_dict()
    )

    results = []
    for _, row in universe_df.iterrows():
        sic = row.get("sic")
        naics = sic_to_naics(sic) if pd.notna(sic) else None
        soc_groups = sic_to_soc_groups(sic) if pd.notna(sic) else []

        if soc_groups:
            scores = [soc_to_score.get(g, 0.15) for g in soc_groups]
            avg_score = sum(scores) / len(scores)
        else:
            avg_score = 0.15  # default mid-range

        results.append(
            {
                "ticker": row["ticker"],
                "sic": sic,
                "naics": naics,
                "soc_groups": ",".join(soc_groups),
                "avg_ai_applicability": round(avg_score, 4),
            }
        )

    return pd.DataFrame(results)
