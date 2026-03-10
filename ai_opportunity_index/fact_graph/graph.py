"""Fact Graph — in-memory graph store with inference support."""

from __future__ import annotations

from datetime import datetime
from typing import Any
import logging
import uuid

from .models import (
    FactNode, FactEdge, FactAttribute, Provenance, Constraint,
    CounterfactualBranch, InferenceResult, InferenceMethod,
    EntityType, RelationType, ProvenanceType, FactStatus,
)

logger = logging.getLogger(__name__)


class FactGraph:
    """In-memory fact graph supporting probabilistic attributes and inference.

    The graph consists of:
    - Nodes (FactNode): Entities with probabilistic attributes
    - Edges (FactEdge): Relationships between entities
    - Constraints: Logical rules for Sudoku-style inference
    - Branches: Counterfactual alternate realities
    """

    def __init__(self):
        self.nodes: dict[str, FactNode] = {}
        self.edges: dict[str, FactEdge] = {}
        self.constraints: dict[str, Constraint] = {}
        self.branches: dict[str, CounterfactualBranch] = {}
        self._edge_index_source: dict[str, list[str]] = {}  # source_id -> [edge_ids]
        self._edge_index_target: dict[str, list[str]] = {}  # target_id -> [edge_ids]
        self._type_index: dict[EntityType, list[str]] = {}  # entity_type -> [node_ids]

    # -- Node Operations ---------------------------------------------------

    def add_node(self, node: FactNode) -> FactNode:
        self.nodes[node.id] = node
        self._type_index.setdefault(node.entity_type, []).append(node.id)
        return node

    def get_node(self, node_id: str) -> FactNode | None:
        return self.nodes.get(node_id)

    def find_nodes(self, entity_type: EntityType | None = None,
                   label_contains: str | None = None,
                   branch_id: str | None = None) -> list[FactNode]:
        results = list(self.nodes.values())
        if entity_type:
            node_ids = set(self._type_index.get(entity_type, []))
            results = [n for n in results if n.id in node_ids]
        if label_contains:
            lc = label_contains.lower()
            results = [n for n in results if lc in n.label.lower()]
        if branch_id is not None:
            results = [n for n in results if n.branch_id == branch_id]
        return results

    def update_attribute(self, node_id: str, attr_name: str, value: Any,
                         p_true: float = 1.0, provenance: Provenance | None = None,
                         method: InferenceMethod = InferenceMethod.OBSERVED) -> FactAttribute | None:
        node = self.get_node(node_id)
        if not node:
            return None
        return node.set_attr(attr_name, value, p_true, provenance, method)

    # -- Edge Operations ---------------------------------------------------

    def add_edge(self, edge: FactEdge) -> FactEdge:
        self.edges[edge.id] = edge
        self._edge_index_source.setdefault(edge.source_id, []).append(edge.id)
        self._edge_index_target.setdefault(edge.target_id, []).append(edge.id)
        return edge

    def get_edges_from(self, node_id: str, relation: RelationType | None = None) -> list[FactEdge]:
        edge_ids = self._edge_index_source.get(node_id, [])
        edges = [self.edges[eid] for eid in edge_ids if eid in self.edges]
        if relation:
            edges = [e for e in edges if e.relation == relation]
        return edges

    def get_edges_to(self, node_id: str, relation: RelationType | None = None) -> list[FactEdge]:
        edge_ids = self._edge_index_target.get(node_id, [])
        edges = [self.edges[eid] for eid in edge_ids if eid in self.edges]
        if relation:
            edges = [e for e in edges if e.relation == relation]
        return edges

    def find_confirmations(self, node_id: str, attr_name: str) -> list[tuple[FactEdge, FactNode]]:
        """Find edges that CONFIRM facts about this node from other sources."""
        result = []
        for edge in self.get_edges_to(node_id, RelationType.CONFIRMS):
            source = self.get_node(edge.source_id)
            if source:
                result.append((edge, source))
        return result

    def find_contradictions(self, node_id: str) -> list[tuple[FactEdge, FactNode]]:
        """Find edges that CONTRADICT facts about this node."""
        result = []
        for edge in self.get_edges_to(node_id, RelationType.CONTRADICTS):
            source = self.get_node(edge.source_id)
            if source:
                result.append((edge, source))
        return result

    # -- Constraint Operations ---------------------------------------------

    def add_constraint(self, constraint: Constraint) -> Constraint:
        self.constraints[constraint.id] = constraint
        return constraint

    # -- Counterfactual Operations -----------------------------------------

    def create_branch(self, name: str, description: str, hypothesis: str,
                      parent_branch_id: str | None = None) -> CounterfactualBranch:
        branch = CounterfactualBranch(
            name=name, description=description, hypothesis=hypothesis,
            parent_branch_id=parent_branch_id,
        )
        self.branches[branch.id] = branch
        return branch

    def fork_node(self, node_id: str, branch_id: str) -> FactNode | None:
        """Create a counterfactual copy of a node on a branch."""
        original = self.get_node(node_id)
        if not original:
            return None
        forked = original.model_copy(deep=True)
        forked.id = str(uuid.uuid4())
        forked.branch_id = branch_id
        forked.status = FactStatus.HYPOTHETICAL
        self.add_node(forked)
        return forked

    # -- Statistics --------------------------------------------------------

    def stats(self) -> dict:
        total_attrs = sum(len(n.attributes) for n in self.nodes.values())
        missing = sum(len(n.missing_attributes()) for n in self.nodes.values())
        low_conf = sum(
            len(n.low_confidence_attributes()) for n in self.nodes.values()
        )
        main_nodes = [n for n in self.nodes.values() if n.branch_id is None]
        hypo_nodes = [n for n in self.nodes.values() if n.branch_id is not None]

        return {
            "total_nodes": len(self.nodes),
            "main_reality_nodes": len(main_nodes),
            "hypothetical_nodes": len(hypo_nodes),
            "total_edges": len(self.edges),
            "total_attributes": total_attrs,
            "missing_values": missing,
            "low_confidence_values": low_conf,
            "total_constraints": len(self.constraints),
            "counterfactual_branches": len(self.branches),
            "nodes_by_type": {
                t.value: len(ids) for t, ids in self._type_index.items()
            },
        }

    # -- Serialization -----------------------------------------------------

    def to_dict(self) -> dict:
        return {
            "nodes": {nid: n.model_dump(mode="json") for nid, n in self.nodes.items()},
            "edges": {eid: e.model_dump(mode="json") for eid, e in self.edges.items()},
            "constraints": {cid: c.model_dump(mode="json") for cid, c in self.constraints.items()},
            "branches": {bid: b.model_dump(mode="json") for bid, b in self.branches.items()},
        }

    @classmethod
    def from_dict(cls, data: dict) -> FactGraph:
        graph = cls()
        for nid, ndata in data.get("nodes", {}).items():
            graph.add_node(FactNode(**ndata))
        for eid, edata in data.get("edges", {}).items():
            graph.add_edge(FactEdge(**edata))
        for cid, cdata in data.get("constraints", {}).items():
            graph.add_constraint(Constraint(**cdata))
        for bid, bdata in data.get("branches", {}).items():
            graph.branches[bid] = CounterfactualBranch(**bdata)
        return graph
