"""Project synthesis: merge evidence groups into discrete investment projects.

Takes all evidence groups and their final valuations for a company and
produces a list of SynthesizedProject records via a single LLM call.
"""

import asyncio
import logging
from datetime import date

from pydantic import BaseModel, Field

from ai_opportunity_index.domains import (
    EvidenceGroup,
    SynthesizedProject,
    Valuation,
)
from ai_opportunity_index.prompts.loader import load_prompt

logger = logging.getLogger(__name__)


# ── Pydantic output schema for the LLM ──────────────────────────────────

class ProjectOutput(BaseModel):
    short_title: str
    description: str
    target_dimension: str
    target_subcategory: str
    target_detail: str = ""
    status: str = "planned"
    dollar_total: float | None = None
    dollar_low: float | None = None
    dollar_high: float | None = None
    confidence: float = 0.5
    evidence_count: int = 0
    date_start: str | None = None
    date_end: str | None = None
    technology_area: str = ""
    deployment_scope: str = ""
    evidence_group_ids: list[int] = Field(default_factory=list)


class SynthesisOutput(BaseModel):
    projects: list[ProjectOutput]


# ── LLM Agent ────────────────────────────────────────────────────────────

def _get_agent():
    from ai_opportunity_index.llm_backend import get_agent

    return get_agent(
        output_type=SynthesisOutput,
        system_prompt=(
            "You are a senior AI investment analyst. Synthesize corporate AI evidence "
            "into discrete investment projects. Be analytical and precise. Return valid JSON "
            "matching the requested schema."
        ),
    )


# ── Build prompt context ─────────────────────────────────────────────────

def _build_group_context(
    groups: list[EvidenceGroup],
    valuations: list[Valuation],
) -> list[dict]:
    """Build the context list for the prompt template."""
    val_by_group: dict[int, Valuation] = {}
    for v in valuations:
        # Prefer final over preliminary; highest dollar_mid for ties
        existing = val_by_group.get(v.group_id)
        if existing is None:
            val_by_group[v.group_id] = v
        elif v.stage == "final" and existing.stage != "final":
            val_by_group[v.group_id] = v
        elif v.stage == existing.stage and (v.dollar_mid or 0) > (existing.dollar_mid or 0):
            val_by_group[v.group_id] = v

    items = []
    for g in groups:
        val = val_by_group.get(g.id)
        item = {
            "group_id": g.id,
            "target_dimension": g.target_dimension or "general",
            "representative_text": (g.representative_text or "")[:600],
            "evidence_type": g.evidence_type or "unknown",
            "passage_count": g.passage_count,
            "confidence": round(g.mean_confidence or 0, 2),
            "date_earliest": str(g.date_earliest) if g.date_earliest else "unknown",
            "date_latest": str(g.date_latest) if g.date_latest else "unknown",
            "valuation": val is not None,
            "valuation_narrative": (val.narrative[:400] if val and val.narrative else ""),
            "dollar_low": f"{val.dollar_low:,.0f}" if val and val.dollar_low else "0",
            "dollar_mid": f"{val.dollar_mid:,.0f}" if val and val.dollar_mid else "0",
            "dollar_high": f"{val.dollar_high:,.0f}" if val and val.dollar_high else "0",
            "valuation_stage": val.evidence_type if val else "",
            "specificity": round(val.specificity, 2) if val and val.specificity else "",
            "technology_area": "",
            "deployment_scope": "",
            "timeframe": "",
        }
        # Extract detail fields from valuation
        if val:
            if val.investment_detail:
                item["technology_area"] = val.investment_detail.technology_area or ""
                item["deployment_scope"] = val.investment_detail.deployment_scope or ""
            if val.plan_detail:
                item["timeframe"] = val.plan_detail.timeframe or ""
        items.append(item)
    return items


# ── Main entry point ─────────────────────────────────────────────────────

async def synthesize_projects(
    company_id: int,
    company_name: str,
    ticker: str,
    sector: str,
    revenue: float,
    groups: list[EvidenceGroup],
    valuations: list[Valuation],
    pipeline_run_id: int | None = None,
    semaphore: asyncio.Semaphore | None = None,
) -> list[SynthesizedProject]:
    """Synthesize evidence groups into discrete investment projects via LLM.

    Returns a list of SynthesizedProject domain objects (without DB IDs).
    """
    if not groups:
        return []

    context = _build_group_context(groups, valuations)
    revenue_str = f"${revenue:,.0f}" if revenue else "Unknown"

    prompt = load_prompt(
        "synthesize_projects",
        company_name=company_name,
        ticker=ticker,
        sector=sector or "Unknown",
        revenue_str=revenue_str,
        groups_with_valuations=context,
        total_groups=len(groups),
    )

    from ai_opportunity_index.llm_backend import run_agent_with_retry

    agent = _get_agent()
    try:
        if semaphore:
            async with semaphore:
                result = await run_agent_with_retry(agent, prompt)
        else:
            result = await run_agent_with_retry(agent, prompt)

        usage = result.usage()
        logger.info(
            "Project synthesis [%s]: input=%d output=%d total=%d tokens, %d projects",
            ticker,
            usage.input_tokens or 0,
            usage.output_tokens or 0,
            usage.total_tokens or 0,
            len(result.output.projects),
        )
    except Exception as e:
        logger.error("Project synthesis failed for %s: %s", ticker, e)
        return []

    # Convert LLM output to domain objects
    projects: list[SynthesizedProject] = []
    for p in result.output.projects:
        # Fix dollar unit mismatches: if LLM returns small numbers (e.g. 13.8 meaning $13.8B),
        # detect and scale up.  Only scale when the description *explicitly* mentions
        # "billion" or "million" — do NOT blindly scale small numbers, as many small-cap
        # companies genuinely have small dollar values.
        dollar_total = p.dollar_total
        dollar_low = p.dollar_low
        dollar_high = p.dollar_high
        if dollar_total is not None and dollar_total < 10000 and dollar_total > 0:
            desc_lower = (p.description or "").lower()
            scale = None
            if "billion" in desc_lower:
                scale = 1e9
            elif "million" in desc_lower:
                scale = 1e6
            if scale is not None:
                logger.warning(
                    "Dollar unit fix for '%s': %.2f -> %.0f (scale=%.0f)",
                    p.short_title, dollar_total, dollar_total * scale, scale,
                )
                dollar_total = dollar_total * scale
                if dollar_low is not None:
                    dollar_low = dollar_low * scale
                if dollar_high is not None:
                    dollar_high = dollar_high * scale

        # Parse dates safely
        d_start = _parse_date(p.date_start)
        d_end = _parse_date(p.date_end)

        # Normalize status
        status = p.status.lower().replace(" ", "_")
        if status not in ("planned", "in_progress", "launched"):
            status = "planned"

        # Normalize dimension
        dim = p.target_dimension.lower()
        if dim not in ("cost", "revenue", "general"):
            dim = "general"

        # Collect valuation IDs for the merged groups
        val_ids = []
        for v in valuations:
            if v.group_id in p.evidence_group_ids and v.id:
                val_ids.append(v.id)

        # Filter out noise projects: LLM sometimes generates negative findings
        # like "No [TICKER] Investment in X" for irrelevant news articles
        title_lower = (p.short_title or "").lower()
        desc_lower = (p.description or "").lower()
        if title_lower.startswith("no ") or "not related" in desc_lower[:200] or "not applicable" in desc_lower[:200]:
            logger.info("Skipping noise project: '%s'", p.short_title[:80])
            continue

        projects.append(SynthesizedProject(
            company_id=company_id,
            pipeline_run_id=pipeline_run_id,
            short_title=p.short_title[:200],
            description=p.description,
            target_dimension=dim,
            target_subcategory=p.target_subcategory[:100],
            target_detail=p.target_detail[:200],
            status=status,
            dollar_total=dollar_total,
            dollar_low=dollar_low,
            dollar_high=dollar_high,
            confidence=max(0.0, min(1.0, p.confidence)),
            evidence_count=p.evidence_count,
            date_start=d_start,
            date_end=d_end,
            technology_area=p.technology_area[:100],
            deployment_scope=p.deployment_scope[:200],
            evidence_group_ids=p.evidence_group_ids,
            valuation_ids=val_ids,
        ))

    logger.info(
        "Synthesized %d projects for %s from %d evidence groups",
        len(projects), ticker, len(groups),
    )
    return projects


def _parse_date(s: str | None) -> date | None:
    if not s or s == "null" or s == "unknown":
        return None
    try:
        return date.fromisoformat(s[:10])
    except (ValueError, TypeError):
        return None
