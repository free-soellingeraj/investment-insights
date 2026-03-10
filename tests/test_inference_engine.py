"""Tests for the Fact Graph inference engine — constraint propagation and reactive updates."""

import pytest

from ai_opportunity_index.fact_graph.graph import FactGraph
from ai_opportunity_index.fact_graph.inference import InferenceEngine
from ai_opportunity_index.fact_graph.models import (
    Constraint,
    EntityType,
    FactNode,
    InferenceMethod,
)


def _make_node(graph, node_id, label, attrs=None):
    """Helper to create a node with pre-set attributes."""
    node = FactNode(id=node_id, entity_type=EntityType.COMPANY, label=label)
    if attrs:
        for name, value in attrs.items():
            node.set_attr(name, value, p_true=1.0, method=InferenceMethod.OBSERVED)
    graph.add_node(node)
    return node


# ── Equality Constraints ─────────────────────────────────────────────────


class TestEqualityConstraint:
    def test_propagate_known_to_unknown(self):
        g = FactGraph()
        _make_node(g, "a", "A", {"revenue": 100.0})
        _make_node(g, "b", "B")  # revenue unknown
        g.add_constraint(Constraint(
            name="eq_revenue",
            description="A.revenue = B.revenue",
            constraint_type="equality",
            participating_facts=["a.revenue", "b.revenue"],
            expression="equality",
        ))
        engine = InferenceEngine(g)
        result = engine.run_logical_pass()
        assert result.facts_updated >= 1
        assert g.get_node("b").get_attr("revenue").value == 100.0

    def test_both_known_consistent(self):
        g = FactGraph()
        _make_node(g, "a", "A", {"score": 0.5})
        _make_node(g, "b", "B", {"score": 0.5})
        g.add_constraint(Constraint(
            name="eq_score",
            description="Scores should match",
            constraint_type="equality",
            participating_facts=["a.score", "b.score"],
            expression="equality",
        ))
        engine = InferenceEngine(g)
        result = engine.run_logical_pass()
        c = list(g.constraints.values())[0]
        assert c.is_satisfied is True

    def test_both_known_inconsistent(self):
        g = FactGraph()
        _make_node(g, "a", "A", {"score": 0.5})
        _make_node(g, "b", "B", {"score": 0.9})
        g.add_constraint(Constraint(
            name="eq_mismatch",
            description="Should mismatch",
            constraint_type="equality",
            participating_facts=["a.score", "b.score"],
            expression="equality",
        ))
        engine = InferenceEngine(g)
        engine.run_logical_pass()
        c = list(g.constraints.values())[0]
        assert c.is_satisfied is False

    def test_both_unknown_no_change(self):
        g = FactGraph()
        _make_node(g, "a", "A")
        _make_node(g, "b", "B")
        g.add_constraint(Constraint(
            name="eq_noop",
            description="Both unknown",
            constraint_type="equality",
            participating_facts=["a.x", "b.x"],
            expression="equality",
        ))
        engine = InferenceEngine(g)
        result = engine.run_logical_pass()
        assert result.facts_updated == 0

    def test_chain_propagation(self):
        """A=B and B=C should propagate A's value to C."""
        g = FactGraph()
        _make_node(g, "a", "A", {"val": 42.0})
        _make_node(g, "b", "B")
        _make_node(g, "c", "C")
        g.add_constraint(Constraint(
            name="ab", description="A=B",
            constraint_type="equality",
            participating_facts=["a.val", "b.val"],
            expression="equality",
        ))
        g.add_constraint(Constraint(
            name="bc", description="B=C",
            constraint_type="equality",
            participating_facts=["b.val", "c.val"],
            expression="equality",
        ))
        engine = InferenceEngine(g)
        result = engine.run_logical_pass()
        assert result.facts_updated >= 2
        assert g.get_node("c").get_attr("val").value == 42.0


# ── Sum Constraints ──────────────────────────────────────────────────────


class TestSumConstraint:
    def test_derive_missing_addend(self):
        """A + B = C, know A and C, derive B."""
        g = FactGraph()
        _make_node(g, "a", "A", {"cost": 30.0})
        _make_node(g, "b", "B")  # unknown
        _make_node(g, "c", "C", {"total": 100.0})
        g.add_constraint(Constraint(
            name="sum_total",
            description="cost + other = total",
            constraint_type="sum",
            participating_facts=["a.cost", "b.other", "c.total"],
            expression="sum",
        ))
        engine = InferenceEngine(g)
        result = engine.run_logical_pass()
        assert result.facts_updated >= 1
        assert abs(g.get_node("b").get_attr("other").value - 70.0) < 1e-6

    def test_derive_total(self):
        """A + B = ?, know A and B, derive total."""
        g = FactGraph()
        _make_node(g, "a", "A", {"x": 25.0})
        _make_node(g, "b", "B", {"y": 75.0})
        _make_node(g, "c", "C")
        g.add_constraint(Constraint(
            name="sum_derive",
            description="x + y = total",
            constraint_type="sum",
            participating_facts=["a.x", "b.y", "c.total"],
            expression="sum",
        ))
        engine = InferenceEngine(g)
        result = engine.run_logical_pass()
        assert result.facts_updated >= 1
        assert abs(g.get_node("c").get_attr("total").value - 100.0) < 1e-6

    def test_sum_with_constant_target(self):
        """A + B = 1.0 (constant), know A, derive B."""
        g = FactGraph()
        _make_node(g, "a", "A", {"prob_yes": 0.7})
        _make_node(g, "b", "B")
        g.add_constraint(Constraint(
            name="prob_sum",
            description="Probabilities sum to 1",
            constraint_type="sum",
            participating_facts=["a.prob_yes", "b.prob_no"],
            expression="sum=1.0",
        ))
        engine = InferenceEngine(g)
        result = engine.run_logical_pass()
        assert result.facts_updated >= 1
        assert abs(g.get_node("b").get_attr("prob_no").value - 0.3) < 1e-6

    def test_sum_all_known_satisfied(self):
        g = FactGraph()
        _make_node(g, "a", "A", {"x": 40.0})
        _make_node(g, "b", "B", {"y": 60.0})
        _make_node(g, "c", "C", {"total": 100.0})
        g.add_constraint(Constraint(
            name="sum_check",
            description="Check sum",
            constraint_type="sum",
            participating_facts=["a.x", "b.y", "c.total"],
            expression="sum",
        ))
        engine = InferenceEngine(g)
        engine.run_logical_pass()
        c = list(g.constraints.values())[0]
        assert c.is_satisfied is True

    def test_sum_all_known_violated(self):
        g = FactGraph()
        _make_node(g, "a", "A", {"x": 40.0})
        _make_node(g, "b", "B", {"y": 60.0})
        _make_node(g, "c", "C", {"total": 200.0})
        g.add_constraint(Constraint(
            name="sum_bad",
            description="Sum doesn't match",
            constraint_type="sum",
            participating_facts=["a.x", "b.y", "c.total"],
            expression="sum",
        ))
        engine = InferenceEngine(g)
        engine.run_logical_pass()
        c = list(g.constraints.values())[0]
        assert c.is_satisfied is False

    def test_multiple_unknowns_no_derivation(self):
        """Can't derive when 2+ values are unknown."""
        g = FactGraph()
        _make_node(g, "a", "A")
        _make_node(g, "b", "B")
        _make_node(g, "c", "C", {"total": 100.0})
        g.add_constraint(Constraint(
            name="sum_cant",
            description="Too many unknowns",
            constraint_type="sum",
            participating_facts=["a.x", "b.y", "c.total"],
            expression="sum",
        ))
        engine = InferenceEngine(g)
        result = engine.run_logical_pass()
        assert result.facts_updated == 0


# ── Implication Constraints ──────────────────────────────────────────────


class TestImplicationConstraint:
    def test_antecedent_true_derives_consequent(self):
        g = FactGraph()
        _make_node(g, "a", "A", {"has_ai_product": 1.0})
        _make_node(g, "b", "B")
        g.add_constraint(Constraint(
            name="impl_product",
            description="If has AI product, must have AI strategy",
            constraint_type="implication",
            participating_facts=["a.has_ai_product", "b.has_ai_strategy"],
            expression="implication",
        ))
        engine = InferenceEngine(g)
        result = engine.run_logical_pass()
        assert result.facts_updated >= 1
        assert g.get_node("b").get_attr("has_ai_strategy").value == 1.0

    def test_antecedent_false_trivially_satisfied(self):
        g = FactGraph()
        _make_node(g, "a", "A", {"x": 0.0})
        _make_node(g, "b", "B")
        g.add_constraint(Constraint(
            name="impl_trivial",
            description="False => anything",
            constraint_type="implication",
            participating_facts=["a.x", "b.y"],
            expression="implication",
        ))
        engine = InferenceEngine(g)
        result = engine.run_logical_pass()
        assert result.facts_updated == 0
        c = list(g.constraints.values())[0]
        assert c.is_satisfied is True

    def test_contrapositive(self):
        """If consequent is false, antecedent must be false."""
        g = FactGraph()
        _make_node(g, "a", "A")  # antecedent unknown
        _make_node(g, "b", "B", {"y": 0.0})  # consequent false
        g.add_constraint(Constraint(
            name="impl_contra",
            description="Contrapositive test",
            constraint_type="implication",
            participating_facts=["a.x", "b.y"],
            expression="implication",
        ))
        engine = InferenceEngine(g)
        result = engine.run_logical_pass()
        assert result.facts_updated >= 1
        assert g.get_node("a").get_attr("x").value == 0.0

    def test_violation_detected(self):
        g = FactGraph()
        _make_node(g, "a", "A", {"x": 1.0})  # true
        _make_node(g, "b", "B", {"y": 0.0})  # false — violation!
        g.add_constraint(Constraint(
            name="impl_violation",
            description="Contradiction",
            constraint_type="implication",
            participating_facts=["a.x", "b.y"],
            expression="implication",
        ))
        engine = InferenceEngine(g)
        engine.run_logical_pass()
        c = list(g.constraints.values())[0]
        assert c.is_satisfied is False


# ── Mutex Constraints ────────────────────────────────────────────────────


class TestMutexConstraint:
    def test_derive_last_unknown(self):
        """If all but one are false, the last must be true."""
        g = FactGraph()
        _make_node(g, "a", "A", {"q": 0.0})
        _make_node(g, "b", "B", {"q": 0.0})
        _make_node(g, "c", "C")
        g.add_constraint(Constraint(
            name="mutex_q",
            description="Exactly one quadrant",
            constraint_type="mutex",
            participating_facts=["a.q", "b.q", "c.q"],
            expression="mutex",
        ))
        engine = InferenceEngine(g)
        result = engine.run_logical_pass()
        assert result.facts_updated >= 1
        assert g.get_node("c").get_attr("q").value == 1.0

    def test_one_true_sets_rest_false(self):
        g = FactGraph()
        _make_node(g, "a", "A", {"q": 1.0})
        _make_node(g, "b", "B")
        _make_node(g, "c", "C")
        g.add_constraint(Constraint(
            name="mutex_set_false",
            description="One true, rest should be false",
            constraint_type="mutex",
            participating_facts=["a.q", "b.q", "c.q"],
            expression="mutex",
        ))
        engine = InferenceEngine(g)
        result = engine.run_logical_pass()
        assert result.facts_updated >= 2
        assert g.get_node("b").get_attr("q").value == 0.0
        assert g.get_node("c").get_attr("q").value == 0.0

    def test_violation_multiple_true(self):
        g = FactGraph()
        _make_node(g, "a", "A", {"q": 1.0})
        _make_node(g, "b", "B", {"q": 1.0})
        g.add_constraint(Constraint(
            name="mutex_bad",
            description="Two true",
            constraint_type="mutex",
            participating_facts=["a.q", "b.q"],
            expression="mutex",
        ))
        engine = InferenceEngine(g)
        engine.run_logical_pass()
        c = list(g.constraints.values())[0]
        assert c.is_satisfied is False


# ── Ratio Constraints ────────────────────────────────────────────────────


class TestRatioConstraint:
    def test_derive_b_from_a(self):
        """A / B = 0.5, know A, derive B."""
        g = FactGraph()
        _make_node(g, "a", "A", {"revenue": 100.0})
        _make_node(g, "b", "B")
        g.add_constraint(Constraint(
            name="ratio_test",
            description="Revenue ratio",
            constraint_type="ratio",
            participating_facts=["a.revenue", "b.revenue"],
            expression="ratio=0.5",
        ))
        engine = InferenceEngine(g)
        result = engine.run_logical_pass()
        assert result.facts_updated >= 1
        assert abs(g.get_node("b").get_attr("revenue").value - 200.0) < 1e-6

    def test_derive_a_from_b(self):
        g = FactGraph()
        _make_node(g, "a", "A")
        _make_node(g, "b", "B", {"cost": 50.0})
        g.add_constraint(Constraint(
            name="ratio_rev",
            description="Cost ratio",
            constraint_type="ratio",
            participating_facts=["a.cost", "b.cost"],
            expression="ratio=2.0",
        ))
        engine = InferenceEngine(g)
        result = engine.run_logical_pass()
        assert result.facts_updated >= 1
        assert abs(g.get_node("a").get_attr("cost").value - 100.0) < 1e-6

    def test_both_known_satisfied(self):
        g = FactGraph()
        _make_node(g, "a", "A", {"x": 50.0})
        _make_node(g, "b", "B", {"x": 100.0})
        g.add_constraint(Constraint(
            name="ratio_check",
            description="50/100 = 0.5",
            constraint_type="ratio",
            participating_facts=["a.x", "b.x"],
            expression="ratio=0.5",
        ))
        engine = InferenceEngine(g)
        engine.run_logical_pass()
        c = list(g.constraints.values())[0]
        assert c.is_satisfied is True


# ── Range Constraints ────────────────────────────────────────────────────


class TestRangeConstraint:
    def test_in_range(self):
        g = FactGraph()
        _make_node(g, "a", "A", {"score": 0.7})
        g.add_constraint(Constraint(
            name="range_score",
            description="Score in [0,1]",
            constraint_type="range",
            participating_facts=["a.score"],
            expression="range=0.0,1.0",
        ))
        engine = InferenceEngine(g)
        engine.run_logical_pass()
        c = list(g.constraints.values())[0]
        assert c.is_satisfied is True

    def test_out_of_range(self):
        g = FactGraph()
        _make_node(g, "a", "A", {"score": 1.5})
        g.add_constraint(Constraint(
            name="range_bad",
            description="Score out of bounds",
            constraint_type="range",
            participating_facts=["a.score"],
            expression="range=0.0,1.0",
        ))
        engine = InferenceEngine(g)
        engine.run_logical_pass()
        c = list(g.constraints.values())[0]
        assert c.is_satisfied is False


# ── Reactive Propagation ─────────────────────────────────────────────────


class TestReactivePropagation:
    def test_downstream_tracking(self):
        """Changing A should identify B and C as downstream via constraints."""
        g = FactGraph()
        _make_node(g, "a", "A", {"val": 10.0})
        _make_node(g, "b", "B")
        _make_node(g, "c", "C")
        g.add_constraint(Constraint(
            name="ab", description="A=B",
            constraint_type="equality",
            participating_facts=["a.val", "b.val"],
            expression="equality",
        ))
        g.add_constraint(Constraint(
            name="bc", description="B=C",
            constraint_type="equality",
            participating_facts=["b.val", "c.val"],
            expression="equality",
        ))
        engine = InferenceEngine(g)
        downstream = engine.get_downstream_facts("a.val")
        assert "b.val" in downstream
        assert "c.val" in downstream

    def test_invalidate_downstream(self):
        """When A changes, inferred values for B should be cleared."""
        g = FactGraph()
        _make_node(g, "a", "A", {"val": 10.0})
        _make_node(g, "b", "B")
        g.add_constraint(Constraint(
            name="ab", description="A=B",
            constraint_type="equality",
            participating_facts=["a.val", "b.val"],
            expression="equality",
        ))
        engine = InferenceEngine(g)
        # First propagate to set B
        engine.run_logical_pass()
        b_attr = g.get_node("b").get_attr("val")
        assert b_attr.value == 10.0

        # Now invalidate — should clear the inferred attribute
        invalidated = engine.invalidate_downstream("a.val")
        assert "b.val" in invalidated
        assert g.get_node("b").get_attr("val") is None

    def test_propagate_update_re_derives(self):
        """After an update, propagate_update should re-derive downstream facts."""
        g = FactGraph()
        _make_node(g, "a", "A", {"val": 10.0})
        _make_node(g, "b", "B")
        _make_node(g, "c", "C", {"total": 100.0})
        g.add_constraint(Constraint(
            name="eq_ab", description="A=B",
            constraint_type="equality",
            participating_facts=["a.val", "b.val"],
            expression="equality",
        ))
        g.add_constraint(Constraint(
            name="sum_bc", description="B + gap = total",
            constraint_type="sum",
            participating_facts=["b.val", "b.gap", "c.total"],
            expression="sum",
        ))

        engine = InferenceEngine(g)
        # Initial propagation
        engine.run_logical_pass()
        assert g.get_node("b").get_attr("val").value == 10.0
        assert abs(g.get_node("b").get_attr("gap").value - 90.0) < 1e-6

        # Now change A
        g.get_node("a").set_attr("val", 30.0, p_true=1.0, method=InferenceMethod.OBSERVED)

        # Propagate the update
        result = engine.propagate_update("a.val")
        assert result.facts_updated >= 1
        # B should now be 30, gap should be 70
        assert g.get_node("b").get_attr("val").value == 30.0
        assert abs(g.get_node("b").get_attr("gap").value - 70.0) < 1e-6

    def test_no_overwrite_observed_values(self):
        """Constraint propagation should not overwrite observed values."""
        g = FactGraph()
        _make_node(g, "a", "A", {"val": 10.0})
        _make_node(g, "b", "B", {"val": 99.0})  # observed, different
        g.add_constraint(Constraint(
            name="eq", description="A=B",
            constraint_type="equality",
            participating_facts=["a.val", "b.val"],
            expression="equality",
        ))
        engine = InferenceEngine(g)
        result = engine.run_logical_pass()
        # B should NOT be overwritten since it was observed
        assert g.get_node("b").get_attr("val").value == 99.0
        # But the constraint should be marked violated
        c = list(g.constraints.values())[0]
        assert c.is_satisfied is False


# ── Probabilistic Pass ───────────────────────────────────────────────────


class TestProbabilisticPass:
    def test_consistent_hypothesis_boosts_confidence(self):
        g = FactGraph()
        _make_node(g, "a", "A")
        _make_node(g, "b", "B", {"val": 50.0})
        g.add_constraint(Constraint(
            name="eq", description="A=B",
            constraint_type="equality",
            participating_facts=["a.val", "b.val"],
            expression="equality",
        ))
        engine = InferenceEngine(g)
        result = engine.run_probabilistic_pass("a", "val", 50.0)
        attr = g.get_node("a").get_attr("val")
        assert attr.value == 50.0
        assert attr.p_true > 0.5  # Should be boosted

    def test_inconsistent_hypothesis_reverts(self):
        g = FactGraph()
        _make_node(g, "a", "A", {"val": 100.0})  # observed
        _make_node(g, "b", "B")
        # Constraint says B must equal A, but we'll try to set B to something
        # and also have a range constraint that conflicts
        g.add_constraint(Constraint(
            name="eq", description="A=B",
            constraint_type="equality",
            participating_facts=["a.val", "b.val"],
            expression="equality",
        ))
        g.add_constraint(Constraint(
            name="range", description="B must be small",
            constraint_type="range",
            participating_facts=["b.val"],
            expression="range=0.0,50.0",
        ))
        engine = InferenceEngine(g)
        # First propagate to get B=100
        engine.run_logical_pass()
        # Range constraint should be violated
        range_c = [c for c in g.constraints.values() if c.name == "range"][0]
        assert range_c.is_satisfied is False


# ── Counterfactual ───────────────────────────────────────────────────────


class TestCounterfactual:
    def test_fork_and_modify(self):
        g = FactGraph()
        _make_node(g, "a", "A", {"revenue": 1000.0})
        engine = InferenceEngine(g)
        result = engine.run_counterfactual(
            "what_if_double",
            "What if revenue doubled?",
            {"a": {"revenue": 2000.0}},
        )
        assert result.facts_created >= 1
        # Original should be unchanged
        assert g.get_node("a").get_attr("revenue").value == 1000.0
        # Should have a forked node
        forked = [n for n in g.nodes.values() if n.branch_id is not None]
        assert len(forked) >= 1
        assert forked[0].get_attr("revenue").value == 2000.0


# ── Integration: Multiple Constraint Types ───────────────────────────────


class TestMultiConstraintIntegration:
    def test_complex_derivation_chain(self):
        """
        Company has:
        - revenue = 1000
        - cost_ratio = 0.3 (implies cost = 300 via ratio constraint)
        - profit = revenue - costs (sum constraint)
        - profitable = 1 if profit > 0 (implication, checked via inequality)
        """
        g = FactGraph()
        _make_node(g, "co", "Company", {"revenue": 1000.0})
        _make_node(g, "costs", "Costs")
        _make_node(g, "profit", "Profit")

        # Ratio: costs/revenue = 0.3
        g.add_constraint(Constraint(
            name="cost_ratio",
            description="Costs are 30% of revenue",
            constraint_type="ratio",
            participating_facts=["costs.amount", "co.revenue"],
            expression="ratio=0.3",
        ))

        # Sum: costs + profit = revenue
        g.add_constraint(Constraint(
            name="profit_calc",
            description="profit = revenue - costs",
            constraint_type="sum",
            participating_facts=["costs.amount", "profit.amount", "co.revenue"],
            expression="sum",
        ))

        engine = InferenceEngine(g)
        result = engine.run_logical_pass()

        # Costs should be derived as 300
        costs_val = g.get_node("costs").get_attr("amount")
        assert costs_val is not None
        assert abs(costs_val.value - 300.0) < 1e-6

        # Profit should be derived as 700
        profit_val = g.get_node("profit").get_attr("amount")
        assert profit_val is not None
        assert abs(profit_val.value - 700.0) < 1e-6

        assert result.facts_updated >= 2

    def test_scoring_constraint_network(self):
        """Model the AI scoring system as a constraint network.

        opportunity = mean(cost_opp, revenue_opp)  (sum with constant)
        realization = mean(cost_capture, revenue_capture)
        """
        g = FactGraph()
        _make_node(g, "s", "Score", {
            "cost_opp": 0.6,
            "revenue_opp": 0.8,
        })
        _make_node(g, "agg", "Aggregated")

        # Sum: cost_opp + revenue_opp = 2 * opportunity
        # We'll use: cost_opp + revenue_opp = opp_sum, then ratio opp_sum/opportunity = 2
        g.add_constraint(Constraint(
            name="opp_sum",
            description="Sum of opp scores",
            constraint_type="sum",
            participating_facts=["s.cost_opp", "s.revenue_opp", "agg.opp_sum"],
            expression="sum",
        ))
        g.add_constraint(Constraint(
            name="opp_avg",
            description="opportunity = sum / 2",
            constraint_type="ratio",
            participating_facts=["agg.opp_sum", "agg.opportunity"],
            expression="ratio=2.0",
        ))

        engine = InferenceEngine(g)
        result = engine.run_logical_pass()

        opp_sum = g.get_node("agg").get_attr("opp_sum")
        assert opp_sum is not None
        assert abs(opp_sum.value - 1.4) < 1e-6

        opportunity = g.get_node("agg").get_attr("opportunity")
        assert opportunity is not None
        assert abs(opportunity.value - 0.7) < 1e-6
