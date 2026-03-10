"""Tests for the correctness auditing module."""

import math
from datetime import date, datetime, timedelta

import pytest

from ai_opportunity_index.fact_graph.auditor import (
    AuditFinding,
    AuditReport,
    audit_company,
    audit_dollar_plausibility,
    audit_provenance_completeness,
    audit_quadrant_assignment,
    audit_score_bounds,
    audit_staleness,
)


class TestAuditQuadrant:
    def test_correct_high_high(self):
        assert audit_quadrant_assignment(0.6, 0.6, "high_opp_high_real", 1, "TEST") is None

    def test_correct_high_low(self):
        assert audit_quadrant_assignment(0.6, 0.3, "high_opp_low_real", 1, "TEST") is None

    def test_correct_low_high(self):
        assert audit_quadrant_assignment(0.3, 0.6, "low_opp_high_real", 1, "TEST") is None

    def test_correct_low_low(self):
        assert audit_quadrant_assignment(0.3, 0.3, "low_opp_low_real", 1, "TEST") is None

    def test_mismatch_critical(self):
        finding = audit_quadrant_assignment(0.8, 0.8, "low_opp_low_real", 1, "TEST")
        assert finding is not None
        assert finding.severity == "critical"
        assert finding.expected == "high_opp_high_real"
        assert finding.actual == "low_opp_low_real"

    def test_none_quadrant_warning(self):
        finding = audit_quadrant_assignment(0.5, 0.5, None, 1, "TEST")
        assert finding is not None
        assert finding.severity == "warning"

    def test_boundary_exact_threshold(self):
        # At exactly 0.5, should be high (>= threshold)
        assert audit_quadrant_assignment(0.5, 0.5, "high_opp_high_real", 1, "TEST") is None

    def test_just_below_threshold(self):
        assert audit_quadrant_assignment(0.49, 0.49, "low_opp_low_real", 1, "TEST") is None


class TestAuditScoreBounds:
    def test_valid_scores(self):
        findings = audit_score_bounds(1, "TEST", 0.5, 0.5, 0.3, 0.7, 0.4, 0.6)
        assert len(findings) == 0

    def test_negative_score(self):
        findings = audit_score_bounds(1, "TEST", -0.1, 0.5)
        assert len(findings) == 1
        assert findings[0].severity == "critical"

    def test_over_one_score(self):
        findings = audit_score_bounds(1, "TEST", 0.5, 1.5)
        assert len(findings) == 1
        assert findings[0].severity == "critical"

    def test_nan_score(self):
        findings = audit_score_bounds(1, "TEST", float("nan"), 0.5)
        assert len(findings) >= 1
        assert any(f.description == "opportunity score is NaN or Inf" for f in findings)

    def test_inf_score(self):
        findings = audit_score_bounds(1, "TEST", float("inf"), 0.5)
        assert len(findings) >= 1

    def test_none_subscores_ok(self):
        findings = audit_score_bounds(1, "TEST", 0.5, 0.5, None, None, None, None)
        assert len(findings) == 0

    def test_zero_scores_valid(self):
        findings = audit_score_bounds(1, "TEST", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
        assert len(findings) == 0

    def test_one_scores_valid(self):
        findings = audit_score_bounds(1, "TEST", 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
        assert len(findings) == 0


class TestAuditDollarPlausibility:
    def test_reasonable_dollars(self):
        findings = audit_dollar_plausibility(1, "TEST", 1e6, 5e6, 2e6, 1e9)
        assert len(findings) == 0

    def test_negative_dollars_critical(self):
        findings = audit_dollar_plausibility(1, "TEST", -1e6, None, None)
        assert len(findings) == 1
        assert findings[0].severity == "critical"

    def test_trillion_dollar_warning(self):
        findings = audit_dollar_plausibility(1, "TEST", 2e12, None, None)
        assert any(f.severity == "warning" and "1T" in f.description for f in findings)

    def test_ten_x_revenue_warning(self):
        findings = audit_dollar_plausibility(1, "TEST", 11e9, None, None, company_revenue=1e9)
        assert any("10x" in f.description for f in findings)

    def test_none_dollars_ok(self):
        findings = audit_dollar_plausibility(1, "TEST", None, None, None)
        assert len(findings) == 0


class TestAuditProvenance:
    def test_complete_provenance(self):
        findings = audit_provenance_completeness(1, "TEST", [1, 2], [3, 4], 5)
        assert len(findings) == 0

    def test_missing_group_ids_with_evidence(self):
        findings = audit_provenance_completeness(1, "TEST", None, [1], 5)
        assert any("evidence_group_ids" in f.description for f in findings)

    def test_missing_valuation_ids_with_evidence(self):
        findings = audit_provenance_completeness(1, "TEST", [1], None, 5)
        assert any("valuation_ids" in f.description for f in findings)

    def test_no_evidence_info(self):
        findings = audit_provenance_completeness(1, "TEST", None, None, 0)
        assert len(findings) == 1
        assert findings[0].severity == "info"

    def test_empty_lists_with_evidence(self):
        findings = audit_provenance_completeness(1, "TEST", [], [], 5)
        assert len(findings) == 2  # both missing


class TestAuditStaleness:
    def test_fresh_score(self):
        scored_at = datetime.utcnow() - timedelta(days=1)
        findings = audit_staleness(1, "TEST", scored_at)
        assert len(findings) == 0

    def test_warning_score(self):
        scored_at = datetime.utcnow() - timedelta(days=20)
        findings = audit_staleness(1, "TEST", scored_at)
        assert len(findings) == 1
        assert findings[0].severity == "warning"

    def test_critical_score(self):
        scored_at = datetime.utcnow() - timedelta(days=60)
        findings = audit_staleness(1, "TEST", scored_at)
        assert len(findings) == 1
        assert findings[0].severity == "critical"

    def test_no_scored_at(self):
        findings = audit_staleness(1, "TEST", None)
        assert len(findings) == 1
        assert findings[0].severity == "critical"

    def test_evidence_newer_than_score(self):
        scored_at = datetime.utcnow() - timedelta(days=5)
        evidence_date = date.today()
        findings = audit_staleness(1, "TEST", scored_at, evidence_date)
        assert any("newer than the score" in f.description for f in findings)

    def test_evidence_older_than_score_ok(self):
        scored_at = datetime.utcnow()
        evidence_date = date.today() - timedelta(days=10)
        findings = audit_staleness(1, "TEST", scored_at, evidence_date)
        assert not any("newer" in f.description for f in findings)


class TestAuditCompanyIntegration:
    def test_clean_company(self):
        findings = audit_company(
            company_id=1,
            ticker="AAPL",
            opportunity=0.75,
            realization=0.65,
            quadrant="high_opp_high_real",
            cost_opp=0.6,
            revenue_opp=0.8,
            cost_capture=0.5,
            revenue_capture=0.7,
            ai_index_usd=1e6,
            evidence_group_ids=[1, 2, 3],
            valuation_ids=[4, 5, 6],
            evidence_count=10,
            scored_at=datetime.utcnow() - timedelta(days=3),
        )
        # Should have 0 critical/warning, maybe info
        critical = [f for f in findings if f.severity == "critical"]
        warnings = [f for f in findings if f.severity == "warning"]
        assert len(critical) == 0
        assert len(warnings) == 0

    def test_problematic_company(self):
        findings = audit_company(
            company_id=1,
            ticker="BAD",
            opportunity=1.5,  # out of bounds
            realization=-0.1,  # out of bounds
            quadrant="low_opp_low_real",  # mismatch (should be high_opp)
            ai_index_usd=-1e6,  # negative
            evidence_count=10,
            evidence_group_ids=None,  # missing provenance
            scored_at=datetime.utcnow() - timedelta(days=60),  # stale
        )
        critical = [f for f in findings if f.severity == "critical"]
        assert len(critical) >= 3  # bounds, quadrant, staleness at minimum


class TestAuditReport:
    def test_empty_report(self):
        report = AuditReport()
        assert report.is_clean
        assert report.summary()["pass_rate"] == 0.0

    def test_report_with_findings(self):
        report = AuditReport(companies_audited=10, clean_companies=7)
        report.add(AuditFinding("critical", "bounds", 1, "T", "bad"))
        report.add(AuditFinding("warning", "quadrant", 2, "T", "meh"))
        report.add(AuditFinding("info", "provenance", 3, "T", "fyi"))
        assert not report.is_clean
        assert report.critical_count == 1
        assert report.warning_count == 1
        assert report.info_count == 1
        assert report.summary()["pass_rate"] == 70.0
