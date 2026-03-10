"""Comprehensive unit tests for the fact graph module.

Tests cover models, graph operations, inference engine, and bridge functions.
All tests are pure in-memory with no database dependency.
"""

from __future__ import annotations

import pytest

from ai_opportunity_index.fact_graph.models import (
    FactNode, FactEdge, FactAttribute, Provenance,
    EntityType, RelationType, ProvenanceType, InferenceMethod, FactStatus,
)
from ai_opportunity_index.fact_graph.graph import FactGraph
from ai_opportunity_index.fact_graph.inference import InferenceEngine
from ai_opportunity_index.fact_graph.bridge import company_to_fact_node, evidence_to_fact_node


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_company_node(label: str = "Acme Corp", **kwargs) -> FactNode:
    return FactNode(entity_type=EntityType.COMPANY, label=label, **kwargs)


def _make_event_node(label: str = "Evidence: patent", **kwargs) -> FactNode:
    return FactNode(entity_type=EntityType.EVENT, label=label, **kwargs)


def _make_provenance(**overrides) -> Provenance:
    defaults = dict(provenance_type=ProvenanceType.SOURCE)
    defaults.update(overrides)
    return Provenance(**defaults)


# ===========================================================================
# Models
# ===========================================================================

class TestFactAttribute:
    """FactAttribute default values."""

    def test_defaults(self):
        attr = FactAttribute(name="revenue")
        assert attr.value is None
        assert attr.p_true is None
        assert attr.value_type == "string"
        assert attr.provenance == []
        assert attr.inferred_by is None

    def test_with_explicit_values(self):
        prov = _make_provenance()
        attr = FactAttribute(
            name="revenue", value=1_000_000, value_type="float",
            p_true=0.85, provenance=[prov],
        )
        assert attr.value == 1_000_000
        assert attr.p_true == 0.85
        assert len(attr.provenance) == 1


class TestProvenance:
    """Provenance model construction."""

    def test_basic_creation(self):
        prov = Provenance(provenance_type=ProvenanceType.SOURCE, source_url="https://example.com")
        assert prov.provenance_type == ProvenanceType.SOURCE
        assert prov.source_url == "https://example.com"
        assert prov.id  # UUID auto-generated

    def test_defaults(self):
        prov = Provenance(provenance_type=ProvenanceType.DERIVATION)
        assert prov.method == InferenceMethod.OBSERVED
        assert prov.confidence_contribution == 1.0
        assert prov.is_ephemeral is False
        assert prov.parent_fact_ids == []


class TestFactNode:
    """FactNode creation and attribute management."""

    def test_creation_with_entity_type_and_label(self):
        node = FactNode(entity_type=EntityType.COMPANY, label="Apple Inc.")
        assert node.entity_type == EntityType.COMPANY
        assert node.label == "Apple Inc."
        assert node.status == FactStatus.ACTIVE
        assert node.branch_id is None
        assert node.id  # UUID auto-generated

    def test_set_attr_sets_value_p_true_and_provenance(self):
        node = _make_company_node()
        prov = _make_provenance(source_publisher="SEC EDGAR")
        attr = node.set_attr("revenue", 5_000_000, p_true=0.9, provenance=prov)

        assert attr.value == 5_000_000
        assert attr.p_true == 0.9
        assert len(attr.provenance) == 1
        assert attr.provenance[0].source_publisher == "SEC EDGAR"
        assert attr.inferred_by == InferenceMethod.OBSERVED

    def test_set_attr_without_provenance(self):
        node = _make_company_node()
        attr = node.set_attr("ticker", "ACME")
        assert attr.value == "ACME"
        assert attr.provenance == []

    def test_set_attr_with_inference_method(self):
        node = _make_company_node()
        attr = node.set_attr("employees", 500, method=InferenceMethod.PROBABILISTIC)
        assert attr.inferred_by == InferenceMethod.PROBABILISTIC

    def test_get_attr_returns_none_for_missing(self):
        node = _make_company_node()
        assert node.get_attr("nonexistent") is None

    def test_get_attr_returns_attribute(self):
        node = _make_company_node()
        node.set_attr("ticker", "ACME")
        attr = node.get_attr("ticker")
        assert attr is not None
        assert attr.value == "ACME"

    def test_missing_attributes_returns_attrs_with_none_value(self):
        node = _make_company_node()
        node.set_attr("ticker", "ACME")
        node.attributes["revenue"] = FactAttribute(name="revenue", value=None)
        node.attributes["sector"] = FactAttribute(name="sector", value=None)

        missing = node.missing_attributes()
        assert "revenue" in missing
        assert "sector" in missing
        assert "ticker" not in missing

    def test_missing_attributes_empty_when_all_populated(self):
        node = _make_company_node()
        node.set_attr("ticker", "ACME")
        assert node.missing_attributes() == []

    def test_low_confidence_attributes_with_threshold(self):
        node = _make_company_node()
        node.set_attr("revenue", 1_000_000, p_true=0.3)
        node.set_attr("ticker", "ACME", p_true=1.0)
        node.set_attr("sector", "Tech", p_true=0.4)

        low = node.low_confidence_attributes(threshold=0.5)
        assert len(low) == 2
        low_names = {a.name for a in low}
        assert low_names == {"revenue", "sector"}

    def test_low_confidence_skips_none_p_true(self):
        node = _make_company_node()
        node.attributes["unknown"] = FactAttribute(name="unknown", value=None, p_true=None)
        node.set_attr("revenue", 100, p_true=0.2)

        low = node.low_confidence_attributes(threshold=0.5)
        assert len(low) == 1
        assert low[0].name == "revenue"

    def test_low_confidence_default_threshold(self):
        node = _make_company_node()
        node.set_attr("a", "x", p_true=0.49)
        node.set_attr("b", "y", p_true=0.51)

        low = node.low_confidence_attributes()  # default threshold=0.5
        assert len(low) == 1
        assert low[0].name == "a"


class TestFactEdge:
    """FactEdge construction."""

    def test_creation(self):
        edge = FactEdge(source_id="s1", target_id="t1", relation=RelationType.SUPPORTS)
        assert edge.source_id == "s1"
        assert edge.target_id == "t1"
        assert edge.relation == RelationType.SUPPORTS
        assert edge.p_true == 1.0
        assert edge.branch_id is None


# ===========================================================================
# FactGraph
# ===========================================================================

class TestFactGraphNodeOps:
    """Graph node add/get/find operations."""

    def test_add_and_get_roundtrip(self):
        g = FactGraph()
        node = _make_company_node()
        g.add_node(node)
        retrieved = g.get_node(node.id)
        assert retrieved is node

    def test_get_node_returns_none_for_missing(self):
        g = FactGraph()
        assert g.get_node("nonexistent") is None

    def test_find_nodes_by_entity_type(self):
        g = FactGraph()
        g.add_node(_make_company_node("Alpha"))
        g.add_node(_make_company_node("Beta"))
        g.add_node(_make_event_node("Event X"))

        companies = g.find_nodes(entity_type=EntityType.COMPANY)
        assert len(companies) == 2
        events = g.find_nodes(entity_type=EntityType.EVENT)
        assert len(events) == 1

    def test_find_nodes_by_label_contains(self):
        g = FactGraph()
        g.add_node(_make_company_node("Apple Inc."))
        g.add_node(_make_company_node("Banana Corp"))
        g.add_node(_make_company_node("Pineapple Ltd"))

        results = g.find_nodes(label_contains="apple")
        assert len(results) == 2
        labels = {n.label for n in results}
        assert labels == {"Apple Inc.", "Pineapple Ltd"}

    def test_find_nodes_combined_filters(self):
        g = FactGraph()
        g.add_node(_make_company_node("Apple Inc."))
        g.add_node(FactNode(entity_type=EntityType.PRODUCT, label="Apple Watch"))

        results = g.find_nodes(entity_type=EntityType.COMPANY, label_contains="apple")
        assert len(results) == 1
        assert results[0].label == "Apple Inc."


class TestFactGraphEdgeOps:
    """Graph edge add/get operations."""

    def test_add_edge_and_get_edges_from(self):
        g = FactGraph()
        n1 = _make_company_node("A")
        n2 = _make_company_node("B")
        g.add_node(n1)
        g.add_node(n2)

        edge = FactEdge(source_id=n1.id, target_id=n2.id, relation=RelationType.COMPETES_WITH)
        g.add_edge(edge)

        from_edges = g.get_edges_from(n1.id)
        assert len(from_edges) == 1
        assert from_edges[0].target_id == n2.id

    def test_get_edges_to(self):
        g = FactGraph()
        n1 = _make_company_node("A")
        n2 = _make_company_node("B")
        g.add_node(n1)
        g.add_node(n2)

        edge = FactEdge(source_id=n1.id, target_id=n2.id, relation=RelationType.SUPPORTS)
        g.add_edge(edge)

        to_edges = g.get_edges_to(n2.id)
        assert len(to_edges) == 1
        assert to_edges[0].source_id == n1.id

    def test_get_edges_with_relation_filter(self):
        g = FactGraph()
        n1 = _make_company_node("A")
        n2 = _make_company_node("B")
        g.add_node(n1)
        g.add_node(n2)

        g.add_edge(FactEdge(source_id=n1.id, target_id=n2.id, relation=RelationType.SUPPORTS))
        g.add_edge(FactEdge(source_id=n1.id, target_id=n2.id, relation=RelationType.COMPETES_WITH))

        supports = g.get_edges_from(n1.id, relation=RelationType.SUPPORTS)
        assert len(supports) == 1
        competes = g.get_edges_from(n1.id, relation=RelationType.COMPETES_WITH)
        assert len(competes) == 1

    def test_get_edges_from_empty(self):
        g = FactGraph()
        assert g.get_edges_from("no-such-node") == []

    def test_get_edges_to_empty(self):
        g = FactGraph()
        assert g.get_edges_to("no-such-node") == []


class TestFactGraphUpdateAttribute:
    """update_attribute on graph level."""

    def test_update_existing_node(self):
        g = FactGraph()
        node = _make_company_node()
        node.set_attr("revenue", 1_000_000, p_true=0.5)
        g.add_node(node)

        attr = g.update_attribute(node.id, "revenue", 2_000_000, p_true=0.9)
        assert attr is not None
        assert attr.value == 2_000_000
        assert attr.p_true == 0.9
        # Verify reflected on the node
        assert g.get_node(node.id).get_attr("revenue").value == 2_000_000

    def test_update_nonexistent_node_returns_none(self):
        g = FactGraph()
        assert g.update_attribute("missing", "x", 1) is None


class TestFactGraphConfirmationsContradictions:
    """find_confirmations and find_contradictions."""

    def test_find_confirmations_returns_confirms_edges(self):
        g = FactGraph()
        target = _make_company_node("Target")
        source = _make_event_node("Confirming source")
        g.add_node(target)
        g.add_node(source)

        edge = FactEdge(
            source_id=source.id, target_id=target.id,
            relation=RelationType.CONFIRMS,
        )
        g.add_edge(edge)

        confirmations = g.find_confirmations(target.id, "revenue")
        assert len(confirmations) == 1
        returned_edge, returned_node = confirmations[0]
        assert returned_edge.relation == RelationType.CONFIRMS
        assert returned_node.id == source.id

    def test_find_confirmations_empty(self):
        g = FactGraph()
        node = _make_company_node()
        g.add_node(node)
        assert g.find_confirmations(node.id, "revenue") == []

    def test_find_contradictions_returns_contradicts_edges(self):
        g = FactGraph()
        target = _make_company_node("Target")
        source = _make_event_node("Contradicting source")
        g.add_node(target)
        g.add_node(source)

        edge = FactEdge(
            source_id=source.id, target_id=target.id,
            relation=RelationType.CONTRADICTS,
        )
        g.add_edge(edge)

        contradictions = g.find_contradictions(target.id)
        assert len(contradictions) == 1
        returned_edge, returned_node = contradictions[0]
        assert returned_edge.relation == RelationType.CONTRADICTS
        assert returned_node.id == source.id

    def test_find_contradictions_empty(self):
        g = FactGraph()
        node = _make_company_node()
        g.add_node(node)
        assert g.find_contradictions(node.id) == []


class TestFactGraphBranching:
    """Counterfactual branching operations."""

    def test_create_branch(self):
        g = FactGraph()
        branch = g.create_branch(
            name="alt-revenue",
            description="What if revenue doubled?",
            hypothesis="Revenue doubles next quarter",
        )
        assert branch.id in g.branches
        assert branch.name == "alt-revenue"
        assert branch.hypothesis == "Revenue doubles next quarter"

    def test_fork_node(self):
        g = FactGraph()
        original = _make_company_node("Original")
        original.set_attr("revenue", 1_000_000)
        g.add_node(original)

        branch = g.create_branch("b", "test", "hypo")
        forked = g.fork_node(original.id, branch.id)

        assert forked is not None
        assert forked.id != original.id
        assert forked.branch_id == branch.id
        assert forked.status == FactStatus.HYPOTHETICAL
        assert forked.label == original.label
        # Attributes are deep-copied
        assert forked.get_attr("revenue").value == 1_000_000

    def test_fork_node_nonexistent_returns_none(self):
        g = FactGraph()
        branch = g.create_branch("b", "d", "h")
        assert g.fork_node("missing", branch.id) is None

    def test_forked_node_added_to_graph(self):
        g = FactGraph()
        node = _make_company_node()
        g.add_node(node)
        branch = g.create_branch("b", "d", "h")
        forked = g.fork_node(node.id, branch.id)

        assert g.get_node(forked.id) is forked
        assert len(g.nodes) == 2


class TestFactGraphStats:
    """stats() returns correct counts."""

    def test_empty_graph_stats(self):
        g = FactGraph()
        s = g.stats()
        assert s["total_nodes"] == 0
        assert s["total_edges"] == 0
        assert s["total_attributes"] == 0
        assert s["missing_values"] == 0
        assert s["low_confidence_values"] == 0
        assert s["counterfactual_branches"] == 0

    def test_populated_graph_stats(self):
        g = FactGraph()
        n1 = _make_company_node("A")
        n1.set_attr("revenue", 100, p_true=0.3)
        n1.set_attr("ticker", "A", p_true=1.0)
        n1.attributes["sector"] = FactAttribute(name="sector", value=None, p_true=None)
        g.add_node(n1)

        n2 = _make_event_node("Ev")
        g.add_node(n2)

        edge = FactEdge(source_id=n2.id, target_id=n1.id, relation=RelationType.SUPPORTS)
        g.add_edge(edge)

        branch = g.create_branch("b", "d", "h")
        g.fork_node(n1.id, branch.id)

        s = g.stats()
        assert s["total_nodes"] == 3  # n1, n2, forked
        assert s["main_reality_nodes"] == 2
        assert s["hypothetical_nodes"] == 1
        assert s["total_edges"] == 1
        assert s["total_attributes"] >= 3  # at least n1's attrs
        assert s["missing_values"] >= 1  # sector is missing
        assert s["low_confidence_values"] >= 1  # revenue p_true=0.3 < 0.5
        assert s["counterfactual_branches"] == 1
        assert s["nodes_by_type"]["company"] >= 1


class TestFactGraphSerialization:
    """to_dict / from_dict roundtrip."""

    def test_roundtrip_empty_graph(self):
        g = FactGraph()
        data = g.to_dict()
        g2 = FactGraph.from_dict(data)
        assert g2.stats()["total_nodes"] == 0

    def test_roundtrip_with_nodes_edges_branches(self):
        g = FactGraph()
        n1 = _make_company_node("Alpha")
        n1.set_attr("ticker", "ALPH", p_true=1.0)
        g.add_node(n1)

        n2 = _make_event_node("Patent filing")
        g.add_node(n2)

        edge = FactEdge(source_id=n2.id, target_id=n1.id, relation=RelationType.SUPPORTS, p_true=0.8)
        g.add_edge(edge)

        g.create_branch("test-branch", "testing", "what if?")

        data = g.to_dict()
        g2 = FactGraph.from_dict(data)

        assert len(g2.nodes) == 2
        assert len(g2.edges) == 1
        assert len(g2.branches) == 1

        restored = g2.get_node(n1.id)
        assert restored is not None
        assert restored.label == "Alpha"
        assert restored.get_attr("ticker").value == "ALPH"

        restored_edges = g2.get_edges_to(n1.id)
        assert len(restored_edges) == 1
        assert restored_edges[0].relation == RelationType.SUPPORTS

    def test_roundtrip_preserves_stats(self):
        g = FactGraph()
        n = _make_company_node("C")
        n.set_attr("x", 1, p_true=0.9)
        g.add_node(n)

        original_stats = g.stats()
        g2 = FactGraph.from_dict(g.to_dict())
        assert g2.stats()["total_nodes"] == original_stats["total_nodes"]
        assert g2.stats()["total_attributes"] == original_stats["total_attributes"]


# ===========================================================================
# InferenceEngine
# ===========================================================================

class TestInferenceEngineLogical:
    """Logical (Sudoku-style) inference pass."""

    def test_run_logical_pass_no_constraints_zero_updates(self):
        g = FactGraph()
        g.add_node(_make_company_node())
        engine = InferenceEngine(g)

        result = engine.run_logical_pass()
        assert result.facts_updated == 0
        assert result.method == InferenceMethod.LOGICAL

    def test_run_logical_pass_with_constraints_no_updates(self):
        """Constraints exist but evaluator is a no-op stub, so 0 updates."""
        from ai_opportunity_index.fact_graph.models import Constraint
        g = FactGraph()
        g.add_node(_make_company_node())
        g.add_constraint(Constraint(
            name="test", description="test constraint",
            constraint_type="equality", expression="a == b",
        ))
        engine = InferenceEngine(g)
        result = engine.run_logical_pass()
        assert result.facts_updated == 0


class TestInferenceEngineProbabilistic:
    """Probabilistic inference pass."""

    def test_nonexistent_node_returns_empty_result(self):
        g = FactGraph()
        engine = InferenceEngine(g)
        result = engine.run_probabilistic_pass("no-such-id", "revenue", 999)
        assert result.facts_updated == 0
        assert any("not found" in msg for msg in result.reasoning_log)

    def test_sets_hypothesis_value(self):
        g = FactGraph()
        node = _make_company_node("TestCo")
        g.add_node(node)
        engine = InferenceEngine(g)

        result = engine.run_probabilistic_pass(node.id, "revenue", 5_000_000)

        attr = node.get_attr("revenue")
        assert attr is not None
        assert attr.value == 5_000_000
        assert attr.p_true is not None
        assert attr.p_true >= 0.5  # At minimum the initial hypothesis confidence
        assert result.method == InferenceMethod.PROBABILISTIC

    def test_hypothesis_logged(self):
        g = FactGraph()
        node = _make_company_node("TestCo")
        g.add_node(node)
        engine = InferenceEngine(g)
        result = engine.run_probabilistic_pass(node.id, "sector", "Tech")
        assert any("Hypothesized" in msg for msg in result.reasoning_log)


class TestInferenceEngineCounterfactual:
    """Counterfactual inference pass."""

    def test_creates_branch_and_forked_nodes(self):
        g = FactGraph()
        node = _make_company_node("OrigCo")
        node.set_attr("revenue", 1_000_000)
        g.add_node(node)

        engine = InferenceEngine(g)
        result = engine.run_counterfactual(
            branch_name="double-revenue",
            hypothesis="Revenue doubles",
            modifications={node.id: {"revenue": 2_000_000}},
        )

        assert result.method == InferenceMethod.COUNTERFACTUAL
        assert result.facts_created >= 1
        assert len(g.branches) == 1

        # There should be a forked node with the counterfactual value
        branch_id = list(g.branches.keys())[0]
        forked_nodes = g.find_nodes(branch_id=branch_id)
        assert len(forked_nodes) == 1
        assert forked_nodes[0].status == FactStatus.HYPOTHETICAL
        assert forked_nodes[0].get_attr("revenue").value == 2_000_000

    def test_counterfactual_does_not_modify_original(self):
        g = FactGraph()
        node = _make_company_node("OrigCo")
        node.set_attr("revenue", 1_000_000)
        g.add_node(node)

        engine = InferenceEngine(g)
        engine.run_counterfactual(
            branch_name="test",
            hypothesis="test",
            modifications={node.id: {"revenue": 9_999_999}},
        )

        # Original should be unchanged
        assert node.get_attr("revenue").value == 1_000_000

    def test_counterfactual_with_nonexistent_node_skips(self):
        g = FactGraph()
        engine = InferenceEngine(g)
        result = engine.run_counterfactual(
            branch_name="test",
            hypothesis="test",
            modifications={"missing-id": {"x": 1}},
        )
        assert result.facts_created == 0
        assert len(g.branches) == 1  # Branch still created


# ===========================================================================
# Bridge
# ===========================================================================

class TestCompanyToFactNode:
    """company_to_fact_node bridge function."""

    def test_full_dict_all_fields_mapped(self):
        company = {
            "id": 42,
            "ticker": "ACME",
            "company_name": "Acme Corp",
            "sector": "Technology",
            "industry": "Software",
            "sic": "7372",
            "naics": "511210",
            "market_cap": 1_000_000_000,
            "revenue": 500_000_000,
            "employees": 5000,
        }
        node = company_to_fact_node(company)

        assert node.entity_type == EntityType.COMPANY
        assert node.label == "Acme Corp"
        assert node.canonical_id == "42"

        assert node.get_attr("ticker").value == "ACME"
        assert node.get_attr("ticker").p_true == 1.0
        assert node.get_attr("sector").value == "Technology"
        assert node.get_attr("sector").p_true == 0.9
        assert node.get_attr("market_cap").value == 1_000_000_000
        assert node.get_attr("market_cap").p_true == 0.8
        assert node.get_attr("employees").value == 5000
        assert node.get_attr("employees").p_true == 0.7

        # Provenance attached to each present attribute
        for field in ["ticker", "company_name", "sector", "industry",
                      "sic", "naics", "market_cap", "revenue", "employees"]:
            attr = node.get_attr(field)
            assert attr is not None
            assert len(attr.provenance) == 1

    def test_missing_fields_create_missing_attributes(self):
        company = {
            "id": 1,
            "ticker": "X",
            "company_name": "X Corp",
        }
        node = company_to_fact_node(company)

        # Present fields
        assert node.get_attr("ticker").value == "X"

        # Missing fields should be registered with value=None
        missing = node.missing_attributes()
        assert "sector" in missing
        assert "industry" in missing
        assert "market_cap" in missing
        assert "revenue" in missing
        assert "employees" in missing
        assert "sic" in missing
        assert "naics" in missing

    def test_label_falls_back_to_ticker(self):
        node = company_to_fact_node({"ticker": "ZZZ"})
        assert node.label == "ZZZ"

    def test_label_falls_back_to_unknown(self):
        node = company_to_fact_node({})
        assert node.label == "Unknown"


class TestEvidenceToFactNode:
    """evidence_to_fact_node bridge function."""

    def test_creates_node_and_edge(self):
        evidence = {
            "evidence_type": "patent",
            "source_url": "https://patents.example.com/123",
            "source_author": "USPTO",
            "source_publisher": "US Patent Office",
            "target_dimension": "innovation",
            "capture_stage": "filed",
            "source_excerpt": "Method for AI-driven analysis",
            "dollar_estimate_usd": 10_000_000,
            "signal_strength": 0.85,
            "confidence": 0.75,
        }
        company_node_id = "company-abc"
        node, edge = evidence_to_fact_node(evidence, company_node_id)

        assert node.entity_type == EntityType.EVENT
        assert "patent" in node.label

        # Attributes should be set
        assert node.get_attr("target_dimension").value == "innovation"
        assert node.get_attr("capture_stage").value == "filed"
        assert node.get_attr("source_excerpt").value == "Method for AI-driven analysis"
        assert node.get_attr("dollar_estimate_usd").value == 10_000_000
        assert node.get_attr("signal_strength").value == 0.85

        # p_true should match confidence
        for field in ["target_dimension", "capture_stage", "source_excerpt",
                      "dollar_estimate_usd", "signal_strength"]:
            assert node.get_attr(field).p_true == 0.75

    def test_edge_has_supports_relation(self):
        evidence = {
            "evidence_type": "news",
            "target_dimension": "growth",
            "confidence": 0.6,
        }
        node, edge = evidence_to_fact_node(evidence, "comp-1")

        assert edge.relation == RelationType.SUPPORTS
        assert edge.source_id == node.id
        assert edge.target_id == "comp-1"
        assert edge.p_true == 0.6
        assert len(edge.provenance) == 1

    def test_default_confidence(self):
        """When confidence not provided, defaults to 0.5."""
        evidence = {
            "evidence_type": "rumor",
            "signal_strength": 0.3,
        }
        node, edge = evidence_to_fact_node(evidence, "comp-2")
        assert edge.p_true == 0.5
        assert node.get_attr("signal_strength").p_true == 0.5

    def test_missing_fields_not_added(self):
        """Fields not in the evidence dict should not appear as attributes."""
        evidence = {"evidence_type": "patent", "confidence": 0.9}
        node, _ = evidence_to_fact_node(evidence, "comp-3")
        assert node.get_attr("target_dimension") is None
        assert node.get_attr("dollar_estimate_usd") is None
