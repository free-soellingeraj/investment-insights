"""Fact Graph — probabilistic, provenance-tracked knowledge representation.

Provides a graph-based alternative to the existing evidence/scoring system,
with support for logical, probabilistic, and counterfactual inference.
"""

from .models import (
    FactNode,
    FactEdge,
    FactAttribute,
    Provenance,
    Constraint,
    CounterfactualBranch,
    InferenceResult,
    InferenceMethod,
    EntityType,
    RelationType,
    ProvenanceType,
    FactStatus,
)
from .graph import FactGraph
from .inference import InferenceEngine

__all__ = [
    "FactNode",
    "FactEdge",
    "FactAttribute",
    "Provenance",
    "Constraint",
    "CounterfactualBranch",
    "InferenceResult",
    "InferenceMethod",
    "EntityType",
    "RelationType",
    "ProvenanceType",
    "FactStatus",
    "FactGraph",
    "InferenceEngine",
]
