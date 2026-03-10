"""Correctness auditing — verify downstream facts are consistent with upstream evidence.

The CONSTITUTION requires: "Correctness auditing over information trees —
verify that downstream facts remain consistent with upstream evidence."

This module checks:
1. Score-evidence consistency: Do scores reflect the underlying evidence?
2. Dollar estimate bounds: Are dollar estimates within plausible ranges given evidence?
3. Quadrant assignment correctness: Does the quadrant match the scores?
4. Provenance completeness: Does every score have traceable evidence?
5. Temporal consistency: Are newer scores based on newer evidence?
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import date, datetime

from ai_opportunity_index.config import (
    QUADRANT_OPP_THRESHOLD,
    QUADRANT_REAL_THRESHOLD,
    QUADRANT_LABELS,
    SCORE_STALENESS_WARNING_DAYS,
    SCORE_STALENESS_CRITICAL_DAYS,
)

logger = logging.getLogger(__name__)


@dataclass
class AuditFinding:
    """A single audit finding — something that looks wrong or suspicious."""
    severity: str  # "critical", "warning", "info"
    category: str  # "consistency", "bounds", "quadrant", "provenance", "temporal"
    company_id: int
    ticker: str
    description: str
    expected: str | None = None
    actual: str | None = None


@dataclass
class AuditReport:
    """Full audit report for a company or batch."""
    findings: list[AuditFinding] = field(default_factory=list)
    companies_audited: int = 0
    clean_companies: int = 0
    total_findings: int = 0
    critical_count: int = 0
    warning_count: int = 0
    info_count: int = 0

    def add(self, finding: AuditFinding):
        self.findings.append(finding)
        self.total_findings += 1
        if finding.severity == "critical":
            self.critical_count += 1
        elif finding.severity == "warning":
            self.warning_count += 1
        else:
            self.info_count += 1

    @property
    def is_clean(self) -> bool:
        return self.critical_count == 0 and self.warning_count == 0

    def summary(self) -> dict:
        return {
            "companies_audited": self.companies_audited,
            "clean_companies": self.clean_companies,
            "total_findings": self.total_findings,
            "critical": self.critical_count,
            "warning": self.warning_count,
            "info": self.info_count,
            "pass_rate": (
                round(self.clean_companies / self.companies_audited * 100, 1)
                if self.companies_audited > 0 else 0.0
            ),
        }


def audit_quadrant_assignment(
    opportunity: float,
    realization: float,
    quadrant: str | None,
    company_id: int,
    ticker: str,
    opp_threshold: float = QUADRANT_OPP_THRESHOLD,
    real_threshold: float = QUADRANT_REAL_THRESHOLD,
) -> AuditFinding | None:
    """Check that the quadrant label matches the actual scores."""
    if quadrant is None:
        return AuditFinding(
            severity="warning",
            category="quadrant",
            company_id=company_id,
            ticker=ticker,
            description="No quadrant assigned",
        )

    # Determine expected quadrant
    high_opp = opportunity >= opp_threshold
    high_real = realization >= real_threshold

    if high_opp and high_real:
        expected = "high_opp_high_real"
    elif high_opp and not high_real:
        expected = "high_opp_low_real"
    elif not high_opp and high_real:
        expected = "low_opp_high_real"
    else:
        expected = "low_opp_low_real"

    if quadrant != expected:
        return AuditFinding(
            severity="critical",
            category="quadrant",
            company_id=company_id,
            ticker=ticker,
            description="Quadrant mismatch — score says one thing, label says another",
            expected=expected,
            actual=quadrant,
        )
    return None


def audit_score_bounds(
    company_id: int,
    ticker: str,
    opportunity: float,
    realization: float,
    cost_opp: float | None = None,
    revenue_opp: float | None = None,
    cost_capture: float | None = None,
    revenue_capture: float | None = None,
) -> list[AuditFinding]:
    """Check that all scores are within [0, 1] bounds."""
    findings = []
    scores = {
        "opportunity": opportunity,
        "realization": realization,
        "cost_opp": cost_opp,
        "revenue_opp": revenue_opp,
        "cost_capture": cost_capture,
        "revenue_capture": revenue_capture,
    }

    for name, value in scores.items():
        if value is None:
            continue
        if value < 0.0 or value > 1.0:
            findings.append(AuditFinding(
                severity="critical",
                category="bounds",
                company_id=company_id,
                ticker=ticker,
                description=f"{name} score out of [0,1] bounds",
                expected="0.0 <= score <= 1.0",
                actual=str(value),
            ))
        if math.isnan(value) or math.isinf(value):
            findings.append(AuditFinding(
                severity="critical",
                category="bounds",
                company_id=company_id,
                ticker=ticker,
                description=f"{name} score is NaN or Inf",
                actual=str(value),
            ))
    return findings


def audit_dollar_plausibility(
    company_id: int,
    ticker: str,
    ai_index_usd: float | None,
    opportunity_usd: float | None,
    evidence_dollars: float | None,
    company_revenue: float | None = None,
) -> list[AuditFinding]:
    """Check dollar estimates for plausibility."""
    findings = []

    for name, value in [
        ("ai_index_usd", ai_index_usd),
        ("opportunity_usd", opportunity_usd),
        ("evidence_dollars", evidence_dollars),
    ]:
        if value is None:
            continue

        if value < 0:
            findings.append(AuditFinding(
                severity="critical",
                category="bounds",
                company_id=company_id,
                ticker=ticker,
                description=f"{name} is negative",
                actual=f"${value:,.0f}",
            ))

        if value > 1e12:  # > $1 trillion
            findings.append(AuditFinding(
                severity="warning",
                category="bounds",
                company_id=company_id,
                ticker=ticker,
                description=f"{name} exceeds $1T — likely estimation error",
                actual=f"${value:,.0f}",
            ))

        if company_revenue and company_revenue > 0 and value > company_revenue * 10:
            findings.append(AuditFinding(
                severity="warning",
                category="bounds",
                company_id=company_id,
                ticker=ticker,
                description=f"{name} is >10x company revenue",
                expected=f"< ${company_revenue * 10:,.0f}",
                actual=f"${value:,.0f}",
            ))

    return findings


def audit_provenance_completeness(
    company_id: int,
    ticker: str,
    evidence_group_ids: list[int] | None,
    valuation_ids: list[int] | None,
    evidence_count: int,
) -> list[AuditFinding]:
    """Check that scores have traceable provenance."""
    findings = []

    if not evidence_group_ids and evidence_count > 0:
        findings.append(AuditFinding(
            severity="warning",
            category="provenance",
            company_id=company_id,
            ticker=ticker,
            description="Score has evidence but no evidence_group_ids — provenance broken",
        ))

    if not valuation_ids and evidence_count > 0:
        findings.append(AuditFinding(
            severity="warning",
            category="provenance",
            company_id=company_id,
            ticker=ticker,
            description="Score has evidence but no valuation_ids — provenance broken",
        ))

    if evidence_count == 0:
        findings.append(AuditFinding(
            severity="info",
            category="provenance",
            company_id=company_id,
            ticker=ticker,
            description="No evidence — score based on industry/opportunity model only",
        ))

    return findings


def audit_staleness(
    company_id: int,
    ticker: str,
    scored_at: datetime | None,
    latest_evidence_date: date | None = None,
) -> list[AuditFinding]:
    """Check score freshness and evidence-score temporal consistency."""
    findings = []

    if scored_at is None:
        findings.append(AuditFinding(
            severity="critical",
            category="temporal",
            company_id=company_id,
            ticker=ticker,
            description="No scored_at timestamp",
        ))
        return findings

    age_days = (datetime.utcnow() - scored_at).days

    if age_days >= SCORE_STALENESS_CRITICAL_DAYS:
        findings.append(AuditFinding(
            severity="critical",
            category="temporal",
            company_id=company_id,
            ticker=ticker,
            description=f"Score is {age_days} days old (critical threshold: {SCORE_STALENESS_CRITICAL_DAYS})",
        ))
    elif age_days >= SCORE_STALENESS_WARNING_DAYS:
        findings.append(AuditFinding(
            severity="warning",
            category="temporal",
            company_id=company_id,
            ticker=ticker,
            description=f"Score is {age_days} days old (warning threshold: {SCORE_STALENESS_WARNING_DAYS})",
        ))

    # Check temporal consistency: score should be newer than evidence
    if latest_evidence_date and scored_at:
        scored_date = scored_at.date() if isinstance(scored_at, datetime) else scored_at
        if latest_evidence_date > scored_date:
            findings.append(AuditFinding(
                severity="warning",
                category="temporal",
                company_id=company_id,
                ticker=ticker,
                description="Evidence is newer than the score — score may be stale",
                expected=f"scored_at >= {latest_evidence_date}",
                actual=f"scored_at = {scored_date}",
            ))

    return findings


def audit_company(
    company_id: int,
    ticker: str,
    opportunity: float,
    realization: float,
    quadrant: str | None = None,
    cost_opp: float | None = None,
    revenue_opp: float | None = None,
    cost_capture: float | None = None,
    revenue_capture: float | None = None,
    ai_index_usd: float | None = None,
    opportunity_usd: float | None = None,
    evidence_dollars: float | None = None,
    company_revenue: float | None = None,
    evidence_group_ids: list[int] | None = None,
    valuation_ids: list[int] | None = None,
    evidence_count: int = 0,
    scored_at: datetime | None = None,
    latest_evidence_date: date | None = None,
) -> list[AuditFinding]:
    """Run all audits for a single company. Returns list of findings."""
    findings = []

    # Quadrant
    finding = audit_quadrant_assignment(
        opportunity, realization, quadrant, company_id, ticker,
    )
    if finding:
        findings.append(finding)

    # Score bounds
    findings.extend(audit_score_bounds(
        company_id, ticker, opportunity, realization,
        cost_opp, revenue_opp, cost_capture, revenue_capture,
    ))

    # Dollar plausibility
    findings.extend(audit_dollar_plausibility(
        company_id, ticker, ai_index_usd, opportunity_usd,
        evidence_dollars, company_revenue,
    ))

    # Provenance
    findings.extend(audit_provenance_completeness(
        company_id, ticker, evidence_group_ids, valuation_ids, evidence_count,
    ))

    # Staleness
    findings.extend(audit_staleness(
        company_id, ticker, scored_at, latest_evidence_date,
    ))

    return findings
