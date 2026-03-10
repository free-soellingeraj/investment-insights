"""Fact Graph domain models — probabilistic, provenance-tracked knowledge representation.

Every fact in the graph has:
- A value (which may be missing/null)
- A probability p(true) estimating confidence
- A provenance chain tracking how we know this
- An inference method tracking how it was derived

The graph supports three inference strategies:
1. Logical (Sudoku-style): Non-probabilistic constraint propagation
2. Probabilistic: Hypothesize → derive → verify consistency
3. Counterfactual: Fork alternate realities for comparison
"""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

import uuid


class InferenceMethod(str, Enum):
    """How a fact value was determined."""
    OBSERVED = "observed"            # Direct observation from source
    LOGICAL = "logical"              # Non-probabilistic deduction
    PROBABILISTIC = "probabilistic"  # Statistical inference / guess
    COUNTERFACTUAL = "counterfactual"  # Hypothetical reasoning
    AGGREGATED = "aggregated"        # Combined from multiple sources
    HUMAN = "human"                  # Human-provided input


class FactStatus(str, Enum):
    """Lifecycle status of a fact."""
    ACTIVE = "active"          # Currently believed true
    SUPERSEDED = "superseded"  # Replaced by newer fact
    RETRACTED = "retracted"    # Found to be false
    HYPOTHETICAL = "hypothetical"  # Part of a counterfactual branch
    PENDING = "pending"        # Awaiting verification


class ProvenanceType(str, Enum):
    """Type of provenance record."""
    SOURCE = "source"          # Original data source
    DERIVATION = "derivation"  # Logical derivation from other facts
    INFERENCE = "inference"    # Probabilistic inference
    CORRECTION = "correction"  # Human or system correction
    CONFIRMATION = "confirmation"  # Cross-source verification


class EntityType(str, Enum):
    """Types of entities in the fact graph."""
    COMPANY = "company"
    PERSON = "person"
    PRODUCT = "product"
    TECHNOLOGY = "technology"
    MARKET = "market"
    INVESTMENT = "investment"
    THESIS = "thesis"
    TRADE = "trade"
    EVENT = "event"


class RelationType(str, Enum):
    """Types of relationships between entities."""
    OWNS = "owns"
    EMPLOYS = "employs"
    PRODUCES = "produces"
    COMPETES_WITH = "competes_with"
    INVESTS_IN = "invests_in"
    SUPPLIES_TO = "supplies_to"
    PARTNERS_WITH = "partners_with"
    DERIVES_FROM = "derives_from"
    CONFIRMS = "confirms"
    CONTRADICTS = "contradicts"
    SUPPORTS = "supports"


class Provenance(BaseModel):
    """A single provenance record — how we know something."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    provenance_type: ProvenanceType
    source_url: str | None = None
    source_author: str | None = None
    source_publisher: str | None = None
    source_date: date | None = None        # When the source was published
    access_date: date | None = None        # When we accessed the source
    source_authority: str | None = None    # Why the source is credible
    parent_fact_ids: list[str] = Field(default_factory=list)  # Facts this was derived from
    method: InferenceMethod = InferenceMethod.OBSERVED
    reasoning: str | None = None           # How the derivation/inference was done
    confidence_contribution: float = 1.0   # How much this provenance supports p(true)
    is_ephemeral: bool = False             # Was the source URL ephemeral (may go dead)?
    archived_content: str | None = None    # Cached content for ephemeral sources
    created_at: datetime = Field(default_factory=datetime.utcnow)


class FactAttribute(BaseModel):
    """A single attribute of a fact with its own probability and provenance.

    This is the atomic unit of the fact graph. Every attribute can be:
    - Present with a value and p(true)
    - Missing (value=None, p_true=None)
    - Inferred (value set, p_true < 1.0, inferred_by populated)
    """
    name: str                              # Attribute name (e.g., "revenue", "employee_count")
    value: Any = None                      # The attribute value (None = missing)
    value_type: str = "string"             # Type hint: "string", "float", "int", "date", "bool", "json"
    p_true: float | None = None            # P(this value is correct), None = unknown
    provenance: list[Provenance] = Field(default_factory=list)
    inferred_by: InferenceMethod | None = None
    inference_reasoning: str | None = None
    last_verified: datetime | None = None
    valid_from: date | None = None         # Temporal validity start
    valid_to: date | None = None           # Temporal validity end
    constraints: list[str] = Field(default_factory=list)  # Logical constraints this participates in


class FactNode(BaseModel):
    """A node in the fact graph — an entity with probabilistic attributes.

    Represents a real-world entity (company, person, product, etc.) with
    all its known attributes, each tracked independently for provenance
    and probability.
    """
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    entity_type: EntityType
    label: str                             # Human-readable label (e.g., "Apple Inc.")
    canonical_id: str | None = None        # Link to canonical entity (e.g., company DB id)
    attributes: dict[str, FactAttribute] = Field(default_factory=dict)
    status: FactStatus = FactStatus.ACTIVE
    branch_id: str | None = None           # Counterfactual branch (None = main reality)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    def get_attr(self, name: str) -> FactAttribute | None:
        return self.attributes.get(name)

    def set_attr(self, name: str, value: Any, p_true: float = 1.0,
                 provenance: Provenance | None = None,
                 method: InferenceMethod = InferenceMethod.OBSERVED) -> FactAttribute:
        attr = FactAttribute(
            name=name,
            value=value,
            p_true=p_true,
            inferred_by=method,
        )
        if provenance:
            attr.provenance.append(provenance)
        self.attributes[name] = attr
        self.updated_at = datetime.utcnow()
        return attr

    def missing_attributes(self) -> list[str]:
        """Return attribute names that have no value."""
        return [name for name, attr in self.attributes.items() if attr.value is None]

    def low_confidence_attributes(self, threshold: float = 0.5) -> list[FactAttribute]:
        """Return attributes with p(true) below threshold."""
        return [attr for attr in self.attributes.values()
                if attr.p_true is not None and attr.p_true < threshold]


class FactEdge(BaseModel):
    """A directed relationship between two fact nodes."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source_id: str                         # Source node ID
    target_id: str                         # Target node ID
    relation: RelationType
    p_true: float = 1.0                    # Probability this relationship holds
    provenance: list[Provenance] = Field(default_factory=list)
    attributes: dict[str, FactAttribute] = Field(default_factory=dict)  # Edge properties
    branch_id: str | None = None           # Counterfactual branch
    created_at: datetime = Field(default_factory=datetime.utcnow)


class CounterfactualBranch(BaseModel):
    """A hypothetical alternate reality branched from main facts."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str                              # Human-readable name
    description: str                       # What hypothesis is being tested
    parent_branch_id: str | None = None    # Branched from (None = main reality)
    hypothesis: str                        # The assumption being made
    created_at: datetime = Field(default_factory=datetime.utcnow)
    status: str = "active"                 # "active", "confirmed", "rejected", "merged"
    score: float | None = None             # How well this branch explains observations


class Constraint(BaseModel):
    """A logical constraint in the fact graph (for Sudoku-style inference)."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    description: str
    constraint_type: str                   # "equality", "inequality", "sum", "mutex", "implication"
    participating_facts: list[str] = Field(default_factory=list)  # Fact attribute references
    expression: str                        # The constraint expression (e.g., "revenue >= costs")
    is_satisfied: bool | None = None       # None = not yet evaluated


class InferenceResult(BaseModel):
    """Result of running an inference pass on the fact graph."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    method: InferenceMethod
    facts_updated: int = 0
    facts_created: int = 0
    constraints_satisfied: int = 0
    constraints_violated: int = 0
    new_edges: int = 0
    duration_ms: int = 0
    reasoning_log: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)
