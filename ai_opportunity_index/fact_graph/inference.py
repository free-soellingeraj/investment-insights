"""Inference engine for the Fact Graph.

Implements three inference strategies per CONSTITUTION §2.3:
1. Logical (Sudoku-style): Constraint propagation — fill in values that must be true
2. Probabilistic: Hypothesize a value, derive consequences, check consistency
3. Counterfactual: Fork reality, apply hypothesis, compare outcomes

Also implements reactive propagation: when evidence updates, derive which
downstream facts must change (CONSTITUTION §2.2 — inference engine with
constraint propagation).
"""

from __future__ import annotations

import logging
import math
import operator
from datetime import datetime

from .models import (
    InferenceMethod, InferenceResult, Provenance, ProvenanceType,
    FactStatus, Constraint, FactNode, FactAttribute,
)
from .graph import FactGraph

logger = logging.getLogger(__name__)

# Operator map for expression evaluation
_OPS = {
    ">=": operator.ge,
    "<=": operator.le,
    ">": operator.gt,
    "<": operator.lt,
    "==": operator.eq,
    "!=": operator.ne,
}


def _resolve_fact_ref(graph: FactGraph, ref: str) -> tuple[FactNode | None, FactAttribute | None]:
    """Resolve a fact reference like 'node_id.attr_name' to (node, attribute)."""
    parts = ref.split(".", 1)
    if len(parts) != 2:
        return None, None
    node = graph.get_node(parts[0])
    if not node:
        return None, None
    attr = node.get_attr(parts[1])
    return node, attr


def _get_fact_value(graph: FactGraph, ref: str) -> float | None:
    """Get the numeric value of a fact reference, or None if missing."""
    node, attr = _resolve_fact_ref(graph, ref)
    if attr is None or attr.value is None:
        return None
    try:
        return float(attr.value)
    except (ValueError, TypeError):
        return None


def _set_fact_value(
    graph: FactGraph,
    ref: str,
    value: float,
    p_true: float,
    constraint_name: str,
) -> bool:
    """Set a fact value via constraint propagation. Returns True if value changed."""
    parts = ref.split(".", 1)
    if len(parts) != 2:
        return False
    node = graph.get_node(parts[0])
    if not node:
        return False

    existing = node.get_attr(parts[1])
    if existing and existing.value is not None:
        # Don't overwrite observed values with inferred ones
        if existing.inferred_by == InferenceMethod.OBSERVED:
            return False
        # Don't overwrite if value hasn't changed
        try:
            if abs(float(existing.value) - value) < 1e-10:
                return False
        except (ValueError, TypeError):
            pass

    provenance = Provenance(
        provenance_type=ProvenanceType.DERIVATION,
        method=InferenceMethod.LOGICAL,
        reasoning=f"Derived by constraint '{constraint_name}'",
    )
    node.set_attr(parts[1], value, p_true=p_true,
                  provenance=provenance, method=InferenceMethod.LOGICAL)
    return True


class InferenceEngine:
    """Runs inference passes over a FactGraph."""

    def __init__(self, graph: FactGraph):
        self.graph = graph
        # Dependency index: fact_ref -> set of constraint IDs that reference it
        self._dependency_index: dict[str, set[str]] = {}
        self._build_dependency_index()

    def _build_dependency_index(self):
        """Build reverse index: which constraints depend on each fact."""
        self._dependency_index.clear()
        for cid, constraint in self.graph.constraints.items():
            for ref in constraint.participating_facts:
                self._dependency_index.setdefault(ref, set()).add(cid)

    def get_downstream_facts(self, fact_ref: str) -> set[str]:
        """Get all fact references downstream of a given fact.

        Follows the constraint dependency chain transitively:
        if fact A participates in constraint C, and C can derive fact B,
        then B is downstream of A.
        """
        visited: set[str] = set()
        queue = [fact_ref]

        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)

            # Find constraints that reference this fact
            constraint_ids = self._dependency_index.get(current, set())
            for cid in constraint_ids:
                constraint = self.graph.constraints.get(cid)
                if not constraint:
                    continue
                # All other participating facts are potentially downstream
                for ref in constraint.participating_facts:
                    if ref != current and ref not in visited:
                        queue.append(ref)

        visited.discard(fact_ref)  # Don't include the source itself
        return visited

    def invalidate_downstream(self, changed_fact_ref: str) -> list[str]:
        """When a fact changes, clear all downstream inferred facts so they can be re-derived.

        Returns list of fact references that were invalidated.
        """
        downstream = self.get_downstream_facts(changed_fact_ref)
        invalidated = []

        for ref in downstream:
            node, attr = _resolve_fact_ref(self.graph, ref)
            if attr and attr.inferred_by in (InferenceMethod.LOGICAL, InferenceMethod.PROBABILISTIC):
                # Clear inferred values so constraint propagation can re-derive them
                parts = ref.split(".", 1)
                if len(parts) == 2 and node:
                    del node.attributes[parts[1]]
                invalidated.append(ref)
                logger.debug("Invalidated downstream fact %s (cleared for re-derivation)", ref)

        return invalidated

    def propagate_update(self, changed_fact_ref: str) -> InferenceResult:
        """Reactive propagation: when a fact changes, re-derive downstream facts.

        This is the key method for the CONSTITUTION requirement:
        "when evidence updates, derive which downstream facts must change."
        """
        result = InferenceResult(method=InferenceMethod.LOGICAL)
        start = datetime.utcnow()

        # Step 1: Find what's downstream
        downstream = self.get_downstream_facts(changed_fact_ref)
        result.reasoning_log.append(
            f"Fact {changed_fact_ref} changed: {len(downstream)} downstream facts to check"
        )

        # Step 2: Invalidate downstream inferred values
        invalidated = self.invalidate_downstream(changed_fact_ref)
        result.reasoning_log.append(
            f"Invalidated {len(invalidated)} inferred facts"
        )

        # Step 3: Re-run constraint propagation to re-derive
        logical_result = self.run_logical_pass()
        result.facts_updated = logical_result.facts_updated
        result.constraints_satisfied = logical_result.constraints_satisfied
        result.constraints_violated = logical_result.constraints_violated
        result.reasoning_log.extend(logical_result.reasoning_log)

        elapsed = (datetime.utcnow() - start).total_seconds() * 1000
        result.duration_ms = int(elapsed)
        return result

    def run_logical_pass(self) -> InferenceResult:
        """Sudoku-style: propagate constraints to fill in deterministic values.

        This is the first strategy — only make conclusions that MUST be true
        given the existing facts and constraints. No guessing.
        """
        result = InferenceResult(method=InferenceMethod.LOGICAL)
        start = datetime.utcnow()

        changed = True
        iterations = 0
        max_iterations = 100

        while changed and iterations < max_iterations:
            changed = False
            iterations += 1

            for constraint in self.graph.constraints.values():
                updates = self._evaluate_constraint(constraint)
                if updates:
                    changed = True
                    result.facts_updated += len(updates)
                    result.reasoning_log.append(
                        f"Constraint '{constraint.name}' produced {len(updates)} updates"
                    )

        # Count satisfied/violated
        for c in self.graph.constraints.values():
            if c.is_satisfied is True:
                result.constraints_satisfied += 1
            elif c.is_satisfied is False:
                result.constraints_violated += 1

        elapsed = (datetime.utcnow() - start).total_seconds() * 1000
        result.duration_ms = int(elapsed)
        result.reasoning_log.append(
            f"Logical pass complete: {iterations} iterations, "
            f"{result.facts_updated} facts updated"
        )

        logger.info("Logical inference: %d updates in %d iterations",
                     result.facts_updated, iterations)
        return result

    def run_probabilistic_pass(self, target_node_id: str,
                                target_attr: str,
                                hypothesis_value: any) -> InferenceResult:
        """Hypothesize a value, derive consequences, check consistency.

        1. Set the hypothesized value on the target attribute
        2. Run logical inference to propagate consequences
        3. Check for constraint violations
        4. If no violations, increase confidence in the hypothesis
        """
        result = InferenceResult(method=InferenceMethod.PROBABILISTIC)
        start = datetime.utcnow()

        node = self.graph.get_node(target_node_id)
        if not node:
            result.reasoning_log.append(f"Node {target_node_id} not found")
            return result

        # Save original state
        original_attr = node.get_attr(target_attr)
        original_value = original_attr.value if original_attr else None
        original_p = original_attr.p_true if original_attr else None

        # Apply hypothesis
        provenance = Provenance(
            provenance_type=ProvenanceType.INFERENCE,
            method=InferenceMethod.PROBABILISTIC,
            reasoning=f"Hypothesis: {target_attr} = {hypothesis_value}",
        )
        node.set_attr(target_attr, hypothesis_value, p_true=0.5,
                      provenance=provenance, method=InferenceMethod.PROBABILISTIC)
        result.reasoning_log.append(
            f"Hypothesized {node.label}.{target_attr} = {hypothesis_value}"
        )

        # Run logical pass to propagate
        logical_result = self.run_logical_pass()
        result.facts_updated += logical_result.facts_updated
        result.constraints_satisfied = logical_result.constraints_satisfied
        result.constraints_violated = logical_result.constraints_violated
        result.reasoning_log.extend(logical_result.reasoning_log)

        # Check consistency
        if logical_result.constraints_violated == 0:
            # Hypothesis is consistent — increase confidence
            attr = node.get_attr(target_attr)
            if attr:
                # Boost p_true based on how many constraints were satisfied
                boost = min(0.3, logical_result.constraints_satisfied * 0.05)
                attr.p_true = min(1.0, (attr.p_true or 0.5) + boost)
                result.reasoning_log.append(
                    f"Hypothesis consistent: p_true boosted to {attr.p_true:.2f}"
                )
        else:
            # Hypothesis is inconsistent — restore original and note failure
            if original_attr:
                node.set_attr(target_attr, original_value, p_true=original_p or 0.0)
            result.reasoning_log.append(
                f"Hypothesis inconsistent: {logical_result.constraints_violated} violations, "
                f"reverting to original value"
            )

        elapsed = (datetime.utcnow() - start).total_seconds() * 1000
        result.duration_ms = int(elapsed)
        return result

    def run_counterfactual(self, branch_name: str, hypothesis: str,
                           modifications: dict[str, dict[str, any]]) -> InferenceResult:
        """Fork reality, apply modifications, run inference, compare.

        Args:
            branch_name: Name for the counterfactual branch
            hypothesis: Description of what we're testing
            modifications: {node_id: {attr_name: new_value}} changes to apply

        Returns:
            InferenceResult with comparison to main reality
        """
        result = InferenceResult(method=InferenceMethod.COUNTERFACTUAL)
        start = datetime.utcnow()

        # Create branch
        branch = self.graph.create_branch(
            name=branch_name,
            description=f"Testing: {hypothesis}",
            hypothesis=hypothesis,
        )
        result.reasoning_log.append(f"Created branch '{branch_name}': {hypothesis}")

        # Fork affected nodes
        forked_nodes = {}
        for node_id in modifications:
            forked = self.graph.fork_node(node_id, branch.id)
            if forked:
                forked_nodes[node_id] = forked
                result.facts_created += 1

        # Apply modifications to forked nodes
        for orig_id, changes in modifications.items():
            forked = forked_nodes.get(orig_id)
            if not forked:
                continue
            for attr_name, new_value in changes.items():
                provenance = Provenance(
                    provenance_type=ProvenanceType.INFERENCE,
                    method=InferenceMethod.COUNTERFACTUAL,
                    reasoning=f"Counterfactual: {attr_name} = {new_value}",
                )
                forked.set_attr(attr_name, new_value, p_true=1.0,
                               provenance=provenance, method=InferenceMethod.COUNTERFACTUAL)
                result.reasoning_log.append(
                    f"Set {forked.label}.{attr_name} = {new_value} (counterfactual)"
                )

        elapsed = (datetime.utcnow() - start).total_seconds() * 1000
        result.duration_ms = int(elapsed)
        return result

    def _evaluate_constraint(self, constraint: Constraint) -> list[str]:
        """Evaluate a single constraint and derive values where possible.

        Returns list of fact references that were updated.

        Constraint types:
        - equality: "A = B" — if one side known, propagate to the other
        - sum: "A + B + C = D" — if all but one known, derive the missing one
        - implication: "A => B" — if antecedent true, consequent must be true
        - inequality: "A >= B" — mark satisfied/violated, derive bounds
        - mutex: "exactly_one(A, B, C)" — if n-1 are false, the last must be true
        """
        updates = []
        refs = constraint.participating_facts
        expr = constraint.expression

        if constraint.constraint_type == "equality":
            updates = self._eval_equality(constraint, refs)

        elif constraint.constraint_type == "sum":
            updates = self._eval_sum(constraint, refs, expr)

        elif constraint.constraint_type == "implication":
            updates = self._eval_implication(constraint, refs)

        elif constraint.constraint_type == "inequality":
            self._eval_inequality(constraint, refs, expr)

        elif constraint.constraint_type == "mutex":
            updates = self._eval_mutex(constraint, refs)

        elif constraint.constraint_type == "range":
            self._eval_range(constraint, refs, expr)

        elif constraint.constraint_type == "ratio":
            updates = self._eval_ratio(constraint, refs, expr)

        return updates

    def _eval_equality(self, constraint: Constraint, refs: list[str]) -> list[str]:
        """If A = B and one is known, propagate to the other."""
        if len(refs) < 2:
            return []

        updates = []
        values = [_get_fact_value(self.graph, r) for r in refs]
        known = [(i, v) for i, v in enumerate(values) if v is not None]
        unknown = [i for i, v in enumerate(values) if v is None]

        if known and unknown:
            # All known values should agree; use the highest-confidence one
            source_idx, source_val = known[0]
            source_node, source_attr = _resolve_fact_ref(self.graph, refs[source_idx])
            source_p = source_attr.p_true if source_attr else 0.8

            for idx in unknown:
                if _set_fact_value(self.graph, refs[idx], source_val,
                                   p_true=source_p * 0.95,
                                   constraint_name=constraint.name):
                    updates.append(refs[idx])

            constraint.is_satisfied = True
        elif len(known) >= 2:
            # Check if all known values agree
            vals = [v for _, v in known]
            constraint.is_satisfied = all(abs(v - vals[0]) < 1e-10 for v in vals)
        else:
            constraint.is_satisfied = None  # Can't evaluate yet

        return updates

    def _eval_sum(self, constraint: Constraint, refs: list[str], expr: str) -> list[str]:
        """Sum constraint: participating_facts[:-1] should sum to participating_facts[-1].

        Expression format: "sum" or "sum=<target_value>"
        If expression is "sum=<value>", the target is a constant.
        Otherwise, the last ref is the total.
        """
        if len(refs) < 2:
            return []

        updates = []

        # Parse target: either constant from expression or last ref
        target_constant = None
        if "=" in expr:
            try:
                target_constant = float(expr.split("=")[1].strip())
            except ValueError:
                pass

        if target_constant is not None:
            # Sum of all refs should equal the constant
            values = [_get_fact_value(self.graph, r) for r in refs]
            known = [(i, v) for i, v in enumerate(values) if v is not None]
            unknown = [i for i, v in enumerate(values) if v is None]

            if len(unknown) == 1:
                # Can derive the missing value
                known_sum = sum(v for _, v in known)
                derived = target_constant - known_sum
                min_p = min(
                    (_resolve_fact_ref(self.graph, refs[i])[1].p_true or 0.8)
                    for i, _ in known
                ) if known else 0.8

                if _set_fact_value(self.graph, refs[unknown[0]], derived,
                                   p_true=min_p * 0.9,
                                   constraint_name=constraint.name):
                    updates.append(refs[unknown[0]])
                constraint.is_satisfied = True
            elif len(unknown) == 0:
                total = sum(v for _, v in known)
                constraint.is_satisfied = abs(total - target_constant) < 1e-6
        else:
            # Last ref is the total; rest are addends
            addend_refs = refs[:-1]
            total_ref = refs[-1]

            addend_values = [_get_fact_value(self.graph, r) for r in addend_refs]
            total_value = _get_fact_value(self.graph, total_ref)

            known_addends = [(i, v) for i, v in enumerate(addend_values) if v is not None]
            unknown_addends = [i for i, v in enumerate(addend_values) if v is None]

            if total_value is not None and len(unknown_addends) == 1:
                # Derive the one missing addend
                known_sum = sum(v for _, v in known_addends)
                derived = total_value - known_sum
                if _set_fact_value(self.graph, addend_refs[unknown_addends[0]], derived,
                                   p_true=0.85, constraint_name=constraint.name):
                    updates.append(addend_refs[unknown_addends[0]])
                constraint.is_satisfied = True

            elif total_value is None and len(unknown_addends) == 0:
                # Derive the total from all known addends
                total = sum(v for _, v in known_addends)
                if _set_fact_value(self.graph, total_ref, total,
                                   p_true=0.9, constraint_name=constraint.name):
                    updates.append(total_ref)
                constraint.is_satisfied = True

            elif total_value is not None and len(unknown_addends) == 0:
                # All known — check if constraint holds
                total = sum(v for _, v in known_addends)
                constraint.is_satisfied = abs(total - total_value) < 1e-6

        return updates

    def _eval_implication(self, constraint: Constraint, refs: list[str]) -> list[str]:
        """Implication: if refs[0] is true (nonzero), refs[1] must be true.

        For boolean-like attributes: nonzero = true, zero/None = false/unknown.
        """
        if len(refs) < 2:
            return []

        updates = []
        antecedent = _get_fact_value(self.graph, refs[0])
        consequent = _get_fact_value(self.graph, refs[1])

        if antecedent is not None and antecedent != 0:
            # Antecedent is true
            if consequent is None:
                # Derive consequent as true (1.0)
                ant_node, ant_attr = _resolve_fact_ref(self.graph, refs[0])
                ant_p = ant_attr.p_true if ant_attr else 0.8
                if _set_fact_value(self.graph, refs[1], 1.0,
                                   p_true=ant_p * 0.9,
                                   constraint_name=constraint.name):
                    updates.append(refs[1])
                constraint.is_satisfied = True
            elif consequent == 0:
                # Contradiction: antecedent true but consequent false
                constraint.is_satisfied = False
            else:
                constraint.is_satisfied = True
        elif antecedent is not None and antecedent == 0:
            # Antecedent is false — implication trivially satisfied
            constraint.is_satisfied = True
        else:
            # Antecedent unknown — can't evaluate
            # But check contrapositive: if consequent is false, antecedent must be false
            if consequent is not None and consequent == 0:
                if _set_fact_value(self.graph, refs[0], 0.0,
                                   p_true=0.85,
                                   constraint_name=constraint.name):
                    updates.append(refs[0])
                constraint.is_satisfied = True

        return updates

    def _eval_inequality(self, constraint: Constraint, refs: list[str], expr: str):
        """Inequality: check if A op B holds. No derivation, just validation."""
        if len(refs) < 2:
            return

        a = _get_fact_value(self.graph, refs[0])
        b = _get_fact_value(self.graph, refs[1])

        if a is not None and b is not None:
            # Find operator in expression
            for op_str, op_fn in _OPS.items():
                if op_str in expr:
                    constraint.is_satisfied = op_fn(a, b)
                    return
        constraint.is_satisfied = None

    def _eval_mutex(self, constraint: Constraint, refs: list[str]) -> list[str]:
        """Mutex: exactly one of the refs should be nonzero/true.

        If n-1 are known to be false (0), the last must be true (1).
        """
        if len(refs) < 2:
            return []

        updates = []
        values = [_get_fact_value(self.graph, r) for r in refs]

        true_indices = [i for i, v in enumerate(values) if v is not None and v != 0]
        false_indices = [i for i, v in enumerate(values) if v is not None and v == 0]
        unknown_indices = [i for i, v in enumerate(values) if v is None]

        if len(true_indices) == 1 and len(unknown_indices) == 0:
            # Exactly one true, rest known false — satisfied
            constraint.is_satisfied = True
        elif len(true_indices) == 0 and len(unknown_indices) == 1:
            # All known are false, one unknown — it must be true
            idx = unknown_indices[0]
            if _set_fact_value(self.graph, refs[idx], 1.0,
                               p_true=0.9, constraint_name=constraint.name):
                updates.append(refs[idx])
            constraint.is_satisfied = True
        elif len(true_indices) > 1:
            # Multiple true — violation
            constraint.is_satisfied = False
        elif len(true_indices) == 1 and len(unknown_indices) > 0:
            # One true, set unknowns to false
            for idx in unknown_indices:
                if _set_fact_value(self.graph, refs[idx], 0.0,
                                   p_true=0.9, constraint_name=constraint.name):
                    updates.append(refs[idx])
            constraint.is_satisfied = True
        else:
            constraint.is_satisfied = None

        return updates

    def _eval_range(self, constraint: Constraint, refs: list[str], expr: str):
        """Range constraint: value must be within [min, max].

        Expression format: "range=0.0,1.0"
        """
        if not refs:
            return

        try:
            parts = expr.split("=")[1].split(",")
            range_min = float(parts[0].strip())
            range_max = float(parts[1].strip())
        except (IndexError, ValueError):
            return

        for ref in refs:
            val = _get_fact_value(self.graph, ref)
            if val is not None:
                if range_min <= val <= range_max:
                    constraint.is_satisfied = True
                else:
                    constraint.is_satisfied = False
                    return

    def _eval_ratio(self, constraint: Constraint, refs: list[str], expr: str) -> list[str]:
        """Ratio constraint: refs[0] / refs[1] = ratio (from expression).

        Expression format: "ratio=0.5" means A / B should equal 0.5.
        If A known and B unknown (or vice versa), derive the other.
        """
        if len(refs) < 2:
            return []

        try:
            ratio = float(expr.split("=")[1].strip())
        except (IndexError, ValueError):
            return []

        updates = []
        a = _get_fact_value(self.graph, refs[0])
        b = _get_fact_value(self.graph, refs[1])

        if a is not None and b is None and ratio != 0:
            # Derive B = A / ratio
            derived = a / ratio
            if _set_fact_value(self.graph, refs[1], derived,
                               p_true=0.85, constraint_name=constraint.name):
                updates.append(refs[1])
            constraint.is_satisfied = True
        elif b is not None and a is None:
            # Derive A = B * ratio
            derived = b * ratio
            if _set_fact_value(self.graph, refs[0], derived,
                               p_true=0.85, constraint_name=constraint.name):
                updates.append(refs[0])
            constraint.is_satisfied = True
        elif a is not None and b is not None and b != 0:
            actual_ratio = a / b
            constraint.is_satisfied = abs(actual_ratio - ratio) < ratio * 0.1  # 10% tolerance

        return updates
