"""Abstract base classes for the pipeline strategy pattern.

Each stage has a simple deterministic implementation (default) and an
LLM implementation that can be swapped in for paying users.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from ai_opportunity_index.scoring.pipeline.models import (
    EvidencePassage,
    ValuedEvidence,
)


class EvidenceExtractor(ABC):
    """Extract evidence passages from a raw document."""

    @abstractmethod
    def extract(
        self,
        document_text: str,
        source_type: str,
        company_context: dict,
    ) -> list[EvidencePassage]:
        """Extract relevant evidence passages from a document.

        Args:
            document_text: Raw text of the document.
            source_type: Type of document ('filing', 'news', 'patent', 'job').
            company_context: Dict with company info (name, ticker, sector, etc.).

        Returns:
            List of extracted evidence passages.
        """
        ...


class DollarEstimator(ABC):
    """Estimate dollar impact for an evidence passage."""

    @abstractmethod
    def estimate(
        self,
        passage: EvidencePassage,
        company_financials: dict,
    ) -> ValuedEvidence:
        """Estimate the dollar impact of an evidence passage.

        Args:
            passage: The extracted evidence passage.
            company_financials: Dict with revenue, employees, sector, etc.

        Returns:
            ValuedEvidence with dollar estimates.
        """
        ...


class HorizonEstimator(ABC):
    """Estimate the 3-year horizon shape for a valued evidence item."""

    @abstractmethod
    def estimate_horizon(
        self,
        passage: EvidencePassage,
        base_annual_value: float,
    ) -> tuple[float, float, float, str]:
        """Estimate year 1/2/3 values and horizon shape.

        Args:
            passage: The evidence passage with stage info.
            base_annual_value: The base annual dollar impact at full realization.

        Returns:
            Tuple of (year_1, year_2, year_3, shape_name).
        """
        ...
