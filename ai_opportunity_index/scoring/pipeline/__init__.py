"""4-stage evidence-to-dollar scoring pipeline.

Stages:
1. Collect — raw documents (handled by collect_evidence.py)
2. Extract — pull evidence passages from documents
3. Value  — estimate dollar impact per passage
4. Score  — aggregate into company-level metrics
"""

from ai_opportunity_index.scoring.pipeline.models import (
    EvidencePassage,
    ValuedEvidence,
    CompanyDollarScore,
)
from ai_opportunity_index.scoring.pipeline.base import (
    EvidenceExtractor,
    DollarEstimator,
    HorizonEstimator,
)

__all__ = [
    "EvidencePassage",
    "ValuedEvidence",
    "CompanyDollarScore",
    "EvidenceExtractor",
    "DollarEstimator",
    "HorizonEstimator",
]
