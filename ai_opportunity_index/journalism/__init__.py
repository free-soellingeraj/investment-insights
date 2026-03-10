"""Journalism subsystem: Editor, Researcher, Reporter.

The journalism subsystem implements a newsroom metaphor for evidence management:
- Editor: oversees evidence strategy, identifies coverage gaps, reviews quality
- Researcher: discovers and collects raw sources with full provenance
- Reporter: synthesizes evidence into structured narratives (no LLM, template-based)
"""

from .models import (
    Citation,
    CompanyNarrative,
    CoverageReport,
    NarrativeSection,
    ResearchResult,
    ResearchTask,
)
from .editor import Editor
from .researcher import Researcher
from .reporter import Reporter

__all__ = [
    "Citation",
    "CompanyNarrative",
    "CoverageReport",
    "Editor",
    "NarrativeSection",
    "Reporter",
    "ResearchResult",
    "ResearchTask",
    "Researcher",
]
