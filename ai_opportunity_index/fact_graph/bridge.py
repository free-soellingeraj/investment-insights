"""Bridge between the existing evidence/scoring system and the Fact Graph.

Converts existing database records (companies, evidence, scores) into
fact graph nodes and edges, enabling the new inference system to work
alongside the existing pipeline.
"""

from __future__ import annotations

import logging
from datetime import date

from .models import (
    FactNode, FactEdge, FactAttribute, Provenance,
    EntityType, RelationType, ProvenanceType, InferenceMethod,
)
from .graph import FactGraph

logger = logging.getLogger(__name__)


def company_to_fact_node(company_row: dict) -> FactNode:
    """Convert a company database record to a FactNode."""
    node = FactNode(
        entity_type=EntityType.COMPANY,
        label=company_row.get("company_name") or company_row.get("ticker", "Unknown"),
        canonical_id=str(company_row.get("id", "")),
    )

    # Map company fields to probabilistic attributes
    field_mappings = {
        "ticker": ("string", 1.0),
        "company_name": ("string", 1.0),
        "sector": ("string", 0.9),      # Yahoo Finance classification, mostly reliable
        "industry": ("string", 0.9),
        "sic": ("string", 1.0),          # Official SEC data
        "naics": ("string", 1.0),
        "market_cap": ("float", 0.8),    # Changes daily
        "revenue": ("float", 0.85),      # From filings, somewhat dated
        "employees": ("int", 0.7),       # Often estimated
    }

    for field, (vtype, default_p) in field_mappings.items():
        value = company_row.get(field)
        if value is not None:
            provenance = Provenance(
                provenance_type=ProvenanceType.SOURCE,
                source_publisher="SEC EDGAR" if field in ("sic", "naics", "cik") else "Yahoo Finance",
                method=InferenceMethod.OBSERVED,
            )
            node.set_attr(field, value, p_true=default_p, provenance=provenance)
        else:
            # Register the attribute as missing (placeholder for inference)
            node.attributes[field] = FactAttribute(
                name=field,
                value=None,
                value_type=vtype,
                p_true=None,
            )

    return node


def evidence_to_fact_node(evidence_row: dict, company_node_id: str) -> tuple[FactNode, FactEdge]:
    """Convert an evidence record to a FactNode + relationship edge."""
    node = FactNode(
        entity_type=EntityType.EVENT,
        label=f"Evidence: {evidence_row.get('evidence_type', 'unknown')}",
    )

    # Evidence attributes with provenance
    provenance = Provenance(
        provenance_type=ProvenanceType.SOURCE,
        source_url=evidence_row.get("source_url"),
        source_author=evidence_row.get("source_author"),
        source_publisher=evidence_row.get("source_publisher"),
        source_date=evidence_row.get("source_date"),
        access_date=evidence_row.get("source_access_date"),
        source_authority=evidence_row.get("source_authority"),
        method=InferenceMethod.OBSERVED,
    )

    confidence = evidence_row.get("confidence", 0.5)

    for field in ["target_dimension", "capture_stage", "source_excerpt",
                  "dollar_estimate_usd", "signal_strength"]:
        value = evidence_row.get(field)
        if value is not None:
            node.set_attr(field, value, p_true=confidence, provenance=provenance)

    # Create relationship edge
    edge = FactEdge(
        source_id=node.id,
        target_id=company_node_id,
        relation=RelationType.SUPPORTS,
        p_true=confidence,
        provenance=[provenance],
    )

    return node, edge


def build_graph_from_db(session) -> FactGraph:
    """Build a complete FactGraph from the existing database.

    This is the main bridge function that materializes the current DB state
    into a fact graph for inference.
    """
    from ai_opportunity_index.storage.models import CompanyModel, EvidenceModel

    graph = FactGraph()
    company_node_map = {}  # company_id -> node_id

    # Load companies
    companies = session.query(CompanyModel).filter(CompanyModel.is_active == True).all()
    for company in companies:
        company_dict = {
            "id": company.id,
            "ticker": company.ticker,
            "company_name": company.company_name,
            "sector": company.sector,
            "industry": company.industry,
            "sic": company.sic,
            "naics": company.naics,
        }
        node = company_to_fact_node(company_dict)
        graph.add_node(node)
        company_node_map[company.id] = node.id

    # Load latest scores per company and attach to company nodes
    from ai_opportunity_index.storage.models import CompanyScoreModel
    from sqlalchemy import func as sa_func

    latest_subq = (
        session.query(
            CompanyScoreModel.company_id,
            sa_func.max(CompanyScoreModel.scored_at).label("max_scored"),
        )
        .group_by(CompanyScoreModel.company_id)
        .subquery()
    )
    scores = (
        session.query(CompanyScoreModel)
        .join(
            latest_subq,
            (CompanyScoreModel.company_id == latest_subq.c.company_id)
            & (CompanyScoreModel.scored_at == latest_subq.c.max_scored),
        )
        .all()
    )

    for score in scores:
        node_id = company_node_map.get(score.company_id)
        if not node_id:
            continue
        node = graph.get_node(node_id)
        if not node:
            continue

        score_provenance = Provenance(
            provenance_type=ProvenanceType.SOURCE,
            source_publisher="Scoring Pipeline",
            method=InferenceMethod.AGGREGATED,
            reasoning="Composite score from evidence valuation pipeline",
        )

        score_fields = {
            "opportunity": (score.opportunity, "float"),
            "realization": (score.realization, "float"),
            "cost_opp_score": (score.cost_opp_score, "float"),
            "revenue_opp_score": (score.revenue_opp_score, "float"),
            "cost_capture_score": (score.cost_capture_score, "float"),
            "revenue_capture_score": (score.revenue_capture_score, "float"),
            "ai_index_usd": (score.ai_index_usd, "float"),
            "quadrant": (score.quadrant, "string"),
        }
        for field, (value, vtype) in score_fields.items():
            if value is not None:
                node.set_attr(field, value, p_true=0.9, provenance=score_provenance,
                              method=InferenceMethod.AGGREGATED)

    # Load evidence
    evidence_rows = session.query(EvidenceModel).all()
    for ev in evidence_rows:
        company_node_id = company_node_map.get(ev.company_id)
        if not company_node_id:
            continue
        ev_dict = {
            "evidence_type": ev.evidence_type,
            "source_url": ev.source_url,
            "source_date": ev.source_date,
            "target_dimension": ev.target_dimension,
            "capture_stage": ev.capture_stage,
            "source_excerpt": ev.source_excerpt,
            "dollar_estimate_usd": ev.dollar_estimate_usd,
            "signal_strength": ev.signal_strength,
        }
        ev_node, edge = evidence_to_fact_node(ev_dict, company_node_id)
        graph.add_node(ev_node)
        graph.add_edge(edge)

    # Create cross-company competitive edges
    # Companies in the same industry compete
    industry_groups: dict[str, list[str]] = {}
    for node in graph.find_nodes(entity_type=EntityType.COMPANY):
        industry_attr = node.get_attr("industry")
        if industry_attr and industry_attr.value:
            industry_groups.setdefault(industry_attr.value, []).append(node.id)

    for industry, node_ids in industry_groups.items():
        for i, nid_a in enumerate(node_ids):
            for nid_b in node_ids[i+1:]:
                edge = FactEdge(
                    source_id=nid_a,
                    target_id=nid_b,
                    relation=RelationType.COMPETES_WITH,
                    p_true=0.7,  # Same industry ~ competing
                )
                graph.add_edge(edge)

    # Add scoring constraints for each company with scores
    from .models import Constraint

    for company_id, node_id in company_node_map.items():
        node = graph.get_node(node_id)
        if not node:
            continue

        # Range constraints: all scores must be in [0, 1]
        for score_attr in ["opportunity", "realization", "cost_opp_score",
                           "revenue_opp_score", "cost_capture_score",
                           "revenue_capture_score"]:
            if node.get_attr(score_attr) and node.get_attr(score_attr).value is not None:
                graph.add_constraint(Constraint(
                    name=f"{node.label}_{score_attr}_range",
                    description=f"{node.label} {score_attr} must be in [0,1]",
                    constraint_type="range",
                    participating_facts=[f"{node_id}.{score_attr}"],
                    expression="range=0.0,1.0",
                ))

        # AI index must be non-negative
        if node.get_attr("ai_index_usd") and node.get_attr("ai_index_usd").value is not None:
            graph.add_constraint(Constraint(
                name=f"{node.label}_ai_index_nonneg",
                description=f"{node.label} AI index USD must be >= 0",
                constraint_type="range",
                participating_facts=[f"{node_id}.ai_index_usd"],
                expression="range=0.0,1000000000000.0",
            ))

    logger.info("Built fact graph: %s", graph.stats())
    return graph
