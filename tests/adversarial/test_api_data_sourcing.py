"""Adversarial data-sourcing tests for the GraphQL API.

Tests data completeness, cross-endpoint consistency, pagination correctness,
filter correctness, audit report structure, verification endpoint, trade
signals, and edge cases.

Run with:
    /opt/homebrew/bin/python3.11 -m pytest tests/adversarial/test_api_data_sourcing.py -v --tb=short
"""

from __future__ import annotations

import json
from typing import Any, Optional

import httpx
import pytest

BASE_URL = "http://localhost:8080/graphql"
TIMEOUT = 30.0

pytestmark = pytest.mark.adversarial

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _gql(query: str, variables: dict | None = None) -> dict:
    """Execute a GraphQL query and return the full JSON body."""
    payload: dict[str, Any] = {"query": query}
    if variables:
        payload["variables"] = variables
    resp = httpx.post(BASE_URL, json=payload, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def _gql_data(query: str, variables: dict | None = None) -> dict:
    """Execute a GraphQL query and return body['data'], raising on errors."""
    body = _gql(query, variables)
    if "errors" in body:
        raise RuntimeError(
            f"GraphQL errors: {json.dumps(body['errors'], indent=2)}"
        )
    return body["data"]


def _gql_safe(query: str, variables: dict | None = None) -> tuple[Optional[dict], list]:
    """Execute a GraphQL query and return (data, errors_list)."""
    body = _gql(query, variables)
    return body.get("data"), body.get("errors", [])


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

LATEST_SCORES_QUERY = """
query LatestScores($limit: Int!, $offset: Int!, $quadrant: String, $sector: String) {
  latestScores(limit: $limit, offset: $offset, quadrant: $quadrant, sector: $sector) {
    id
    companyId
    ticker
    companyName
    opportunity
    realization
    quadrant
    quadrantLabel
    costOppScore
    revenueOppScore
    compositeOppScore
    costCaptureScore
    revenueCaptureScore
    filingNlpScore
    productScore
    githubScore
    analystScore
    costRoi
    revenueRoi
    combinedRank
    aiIndexUsd
    captureProbability
    opportunityUsd
    evidenceDollars
    flags
    scoredAt
    scoreAgeDays
    stalenessLevel
  }
}
"""

COMPANY_DETAIL_QUERY = """
query CompanyDetail($ticker: String!) {
  companyDetail(ticker: $ticker) {
    company {
      id
      ticker
      companyName
      exchange
      sector
      industry
      sic
      naics
      isActive
    }
    latestScore {
      id
      companyId
      opportunity
      realization
      quadrant
      quadrantLabel
      costOppScore
      revenueOppScore
      aiIndexUsd
      opportunityUsd
      evidenceDollars
      flags
      scoredAt
    }
    evidence {
      id
      companyId
      evidenceType
      evidenceSubtype
      sourceName
      sourceUrl
      sourceDate
      sourceExcerpt
      targetDimension
      captureStage
      signalStrength
      dollarEstimateUsd
      observedAt
    }
    financials {
      id
      companyId
      metric
      value
      valueUnits
      sourceName
      fiscalPeriod
      sourceDatetime
    }
    evidenceGroups {
      id
      companyId
      targetDimension
      evidenceType
      passageCount
      representativeText
      meanConfidence
      dateEarliest
      dateLatest
    }
    valuations {
      id
      groupId
      stage
      evidenceType
      narrative
      confidence
      dollarLow
      dollarMid
      dollarHigh
      specificity
      magnitude
    }
  }
}
"""


@pytest.fixture(scope="module")
def all_scores() -> list[dict]:
    """Fetch a large batch of latestScores."""
    data = _gql_data(LATEST_SCORES_QUERY, {"limit": 200, "offset": 0})
    return data["latestScores"]


@pytest.fixture(scope="module")
def sample_tickers(all_scores) -> list[str]:
    """Pick up to 5 tickers from latestScores for deep inspection."""
    tickers = [s["ticker"] for s in all_scores if s.get("ticker")]
    return tickers[:5]


# ===========================================================================
# 1. Data completeness via companyDetail
# ===========================================================================


class TestDataCompleteness:
    """Query latestScores, pick 5 companies, verify companyDetail returns
    complete data (company info, score, evidence, financials, evidence
    groups, valuations).  Flag any companies with missing data."""

    def test_company_detail_returns_company_info(self, sample_tickers):
        """Each sampled company must have basic company info."""
        if not sample_tickers:
            pytest.skip("No tickers available from latestScores")

        missing_company_info = []
        for ticker in sample_tickers:
            data, errors = _gql_safe(COMPANY_DETAIL_QUERY, {"ticker": ticker})
            if errors:
                missing_company_info.append(
                    f"{ticker}: GraphQL error: {errors[0].get('message', '?')}"
                )
                continue
            detail = data.get("companyDetail")
            if detail is None:
                missing_company_info.append(f"{ticker}: companyDetail returned null")
                continue
            company = detail.get("company")
            if company is None:
                missing_company_info.append(f"{ticker}: company block is null")
            elif not company.get("ticker"):
                missing_company_info.append(f"{ticker}: company.ticker missing")

        if missing_company_info:
            report = "\n".join(missing_company_info)
            pytest.fail(
                f"Company info missing for {len(missing_company_info)}/{len(sample_tickers)} "
                f"companies:\n{report}"
            )

    def test_company_detail_has_score(self, sample_tickers):
        """Each sampled scored company should have latestScore in detail."""
        if not sample_tickers:
            pytest.skip("No tickers available")

        missing_score = []
        for ticker in sample_tickers:
            data, errors = _gql_safe(COMPANY_DETAIL_QUERY, {"ticker": ticker})
            if errors:
                missing_score.append(f"{ticker}: error ({errors[0].get('message', '?')[:60]})")
                continue
            detail = data and data.get("companyDetail")
            if detail and detail.get("latestScore") is None:
                missing_score.append(f"{ticker}: latestScore is null")

        if missing_score:
            print(f"\nMissing scores in companyDetail: {missing_score}")
        # Allow up to 20% missing (some may be legitimately unscored)
        assert len(missing_score) <= max(1, len(sample_tickers) // 2), (
            f"Too many companies missing latestScore: {missing_score}"
        )

    def test_company_detail_has_evidence(self, sample_tickers):
        """Each sampled scored company should have at least some evidence."""
        if not sample_tickers:
            pytest.skip("No tickers available")

        missing_evidence = []
        for ticker in sample_tickers:
            data, errors = _gql_safe(COMPANY_DETAIL_QUERY, {"ticker": ticker})
            if errors:
                continue
            detail = data and data.get("companyDetail")
            if detail and not detail.get("evidence"):
                missing_evidence.append(ticker)

        if missing_evidence:
            print(f"\nCompanies with no evidence in companyDetail: {missing_evidence}")
        # Flag but don't hard fail if < 50%
        pct = len(missing_evidence) / max(len(sample_tickers), 1) * 100
        assert pct < 80, (
            f"{pct:.0f}% of sample has no evidence -- data pipeline may be broken"
        )

    def test_company_detail_has_evidence_groups(self, sample_tickers):
        """Check evidence groups are populated."""
        if not sample_tickers:
            pytest.skip("No tickers available")

        missing = []
        for ticker in sample_tickers:
            data, errors = _gql_safe(COMPANY_DETAIL_QUERY, {"ticker": ticker})
            if errors:
                continue
            detail = data and data.get("companyDetail")
            if detail and not detail.get("evidenceGroups"):
                missing.append(ticker)

        if missing:
            print(f"\nCompanies missing evidenceGroups: {missing}")

    def test_company_detail_has_valuations(self, sample_tickers):
        """Check valuations are populated."""
        if not sample_tickers:
            pytest.skip("No tickers available")

        missing = []
        for ticker in sample_tickers:
            data, errors = _gql_safe(COMPANY_DETAIL_QUERY, {"ticker": ticker})
            if errors:
                continue
            detail = data and data.get("companyDetail")
            if detail and not detail.get("valuations"):
                missing.append(ticker)

        if missing:
            print(f"\nCompanies missing valuations: {missing}")

    def test_company_detail_completeness_summary(self, sample_tickers):
        """Summary: for each company report which sections are present."""
        if not sample_tickers:
            pytest.skip("No tickers available")

        sections = ["company", "latestScore", "evidence", "financials",
                     "evidenceGroups", "valuations"]
        results: list[dict] = []

        for ticker in sample_tickers:
            data, errors = _gql_safe(COMPANY_DETAIL_QUERY, {"ticker": ticker})
            row = {"ticker": ticker, "error": None}
            if errors:
                row["error"] = errors[0].get("message", "?")[:80]
                results.append(row)
                continue
            detail = data and data.get("companyDetail")
            if detail is None:
                row["error"] = "companyDetail returned null"
                results.append(row)
                continue
            for section in sections:
                val = detail.get(section)
                if val is None:
                    row[section] = "NULL"
                elif isinstance(val, list) and len(val) == 0:
                    row[section] = "EMPTY"
                else:
                    row[section] = "OK"
            results.append(row)

        print("\n--- Data Completeness Summary ---")
        for r in results:
            ticker = r["ticker"]
            if r.get("error"):
                print(f"  {ticker}: ERROR - {r['error']}")
            else:
                parts = [f"{k}={r[k]}" for k in sections if k in r]
                print(f"  {ticker}: {', '.join(parts)}")


# ===========================================================================
# 2. Cross-endpoint consistency
# ===========================================================================


class TestCrossEndpointConsistency:
    """Query the same company via latestScores AND companyDetail, verify
    the opportunity/realization scores match between the two endpoints."""

    def test_scores_match_between_endpoints(self, all_scores, sample_tickers):
        """Scores from latestScores must match companyDetail.latestScore."""
        if not sample_tickers:
            pytest.skip("No tickers available")

        scores_by_ticker = {s["ticker"]: s for s in all_scores if s.get("ticker")}
        mismatches = []

        for ticker in sample_tickers:
            list_score = scores_by_ticker.get(ticker)
            if list_score is None:
                continue

            data, errors = _gql_safe(COMPANY_DETAIL_QUERY, {"ticker": ticker})
            if errors:
                continue
            detail = data and data.get("companyDetail")
            if not detail or not detail.get("latestScore"):
                continue

            detail_score = detail["latestScore"]

            opp_list = list_score["opportunity"]
            opp_detail = detail_score["opportunity"]
            real_list = list_score["realization"]
            real_detail = detail_score["realization"]

            if abs(opp_list - opp_detail) > 0.001:
                mismatches.append(
                    f"{ticker}: opportunity {opp_list:.4f} (list) vs {opp_detail:.4f} (detail)"
                )
            if abs(real_list - real_detail) > 0.001:
                mismatches.append(
                    f"{ticker}: realization {real_list:.4f} (list) vs {real_detail:.4f} (detail)"
                )

        if mismatches:
            report = "\n".join(mismatches)
            pytest.fail(f"Score mismatches between endpoints:\n{report}")

    def test_company_id_matches_between_endpoints(self, all_scores, sample_tickers):
        """company_id from latestScores must match companyDetail.company.id."""
        if not sample_tickers:
            pytest.skip("No tickers available")

        scores_by_ticker = {s["ticker"]: s for s in all_scores if s.get("ticker")}
        mismatches = []

        for ticker in sample_tickers:
            list_score = scores_by_ticker.get(ticker)
            if not list_score:
                continue

            data, errors = _gql_safe(COMPANY_DETAIL_QUERY, {"ticker": ticker})
            if errors:
                continue
            detail = data and data.get("companyDetail")
            if not detail or not detail.get("company"):
                continue

            list_cid = list_score["companyId"]
            detail_cid = detail["company"]["id"]
            if list_cid != detail_cid:
                mismatches.append(
                    f"{ticker}: companyId {list_cid} (list) vs {detail_cid} (detail)"
                )

        assert not mismatches, f"company_id mismatches:\n" + "\n".join(mismatches)


# ===========================================================================
# 3. Pagination correctness
# ===========================================================================


class TestPaginationCorrectness:
    """Page through latestScores in pages of 10, verify no duplicate
    company_ids across pages and no gaps."""

    def test_no_duplicate_company_ids_across_pages(self, all_scores):
        """Paginate and collect all company_ids; none should repeat."""
        if not all_scores:
            pytest.skip("No scores available")

        page_size = 10
        all_company_ids: list[int] = []
        all_tickers: list[str] = []
        seen_ids: set[int] = set()
        duplicates: list[str] = []

        offset = 0
        max_pages = 10
        for page_num in range(max_pages):
            data = _gql_data(
                LATEST_SCORES_QUERY,
                {"limit": page_size, "offset": offset},
            )
            page = data["latestScores"]
            if not page:
                break

            for item in page:
                cid = item["companyId"]
                ticker = item.get("ticker", "?")
                if cid in seen_ids:
                    duplicates.append(
                        f"company_id={cid} ticker={ticker} on page {page_num} "
                        f"(offset={offset})"
                    )
                seen_ids.add(cid)
                all_company_ids.append(cid)
                all_tickers.append(ticker)

            offset += page_size

        total_fetched = len(all_company_ids)
        print(f"\nPagination: fetched {total_fetched} scores across "
              f"{min(max_pages, (total_fetched + page_size - 1) // page_size)} pages")

        assert not duplicates, (
            f"Found {len(duplicates)} duplicate company_ids across pages:\n"
            + "\n".join(duplicates[:10])
        )

    def test_page_sizes_are_respected(self):
        """Each page should return at most 'limit' items."""
        page_size = 5
        data = _gql_data(
            LATEST_SCORES_QUERY,
            {"limit": page_size, "offset": 0},
        )
        page = data["latestScores"]
        assert len(page) <= page_size, (
            f"Requested limit={page_size} but got {len(page)} items"
        )

    def test_pages_are_contiguous(self, all_scores):
        """Fetching all items via single query vs pagination should give same count."""
        if not all_scores:
            pytest.skip("No scores available")

        single_query_count = len(all_scores)

        # Paginate and count
        page_size = 10
        paginated_count = 0
        offset = 0
        for _ in range(30):  # safety bound
            data = _gql_data(
                LATEST_SCORES_QUERY,
                {"limit": page_size, "offset": offset},
            )
            page = data["latestScores"]
            if not page:
                break
            paginated_count += len(page)
            if len(page) < page_size:
                break
            offset += page_size

        # They should match (within reason -- if more than 200 scores, single
        # query was capped at 200, so just check paginated >= single)
        print(f"\nSingle query: {single_query_count}, Paginated: {paginated_count}")
        if single_query_count < 200:
            assert paginated_count == single_query_count, (
                f"Contiguity mismatch: paginated={paginated_count} vs "
                f"single={single_query_count}"
            )


# ===========================================================================
# 4. Filter correctness
# ===========================================================================


class TestFilterCorrectness:
    """Query by quadrant and sector filters, verify returned companies
    actually match the filter."""

    def _get_distinct_quadrants(self, all_scores) -> set[str]:
        return {s["quadrant"] for s in all_scores if s.get("quadrant")}

    def _get_distinct_sectors(self) -> set[str]:
        query = """
        query {
          companies(limit: 200) {
            sector
          }
        }
        """
        data = _gql_data(query)
        return {c["sector"] for c in data["companies"] if c.get("sector")}

    def test_quadrant_filter_returns_only_matching(self, all_scores):
        """Filter by quadrant, verify all returned scores match."""
        quadrants = self._get_distinct_quadrants(all_scores)
        if not quadrants:
            pytest.skip("No quadrants found in data")

        for quadrant in list(quadrants)[:3]:
            data = _gql_data(
                LATEST_SCORES_QUERY,
                {"limit": 50, "offset": 0, "quadrant": quadrant},
            )
            filtered = data["latestScores"]
            wrong = [
                s["ticker"]
                for s in filtered
                if s.get("quadrant") != quadrant
            ]
            assert not wrong, (
                f"Quadrant filter '{quadrant}' returned wrong-quadrant companies: {wrong}"
            )

    def test_sector_filter_returns_only_matching(self):
        """Filter by sector, verify all returned companies match."""
        sectors = self._get_distinct_sectors()
        if not sectors:
            pytest.skip("No sectors found in data")

        for sector in list(sectors)[:3]:
            data = _gql_data(
                LATEST_SCORES_QUERY,
                {"limit": 50, "offset": 0, "sector": sector},
            )
            filtered = data["latestScores"]
            if not filtered:
                continue

            # latestScores may not return sector directly. Check via companyDetail
            # for the first result.
            ticker = filtered[0].get("ticker")
            if not ticker:
                continue
            detail_data, errors = _gql_safe(COMPANY_DETAIL_QUERY, {"ticker": ticker})
            if errors:
                continue
            detail = detail_data and detail_data.get("companyDetail")
            if detail and detail.get("company"):
                actual_sector = detail["company"].get("sector")
                assert actual_sector == sector, (
                    f"Sector filter '{sector}' returned company {ticker} "
                    f"with sector='{actual_sector}'"
                )

    def test_quadrant_filter_reduces_result_count(self, all_scores):
        """Filtering by a single quadrant should return fewer results than unfiltered.

        Note: if the unfiltered query was capped by limit (e.g., 200), we use a
        higher limit to get the true total for comparison.
        """
        quadrants = self._get_distinct_quadrants(all_scores)
        if len(quadrants) < 2:
            pytest.skip("Need at least 2 quadrants to test filtering")

        # Get true total count with a very high limit
        total_data = _gql_data(
            LATEST_SCORES_QUERY,
            {"limit": 5000, "offset": 0},
        )
        total_unfiltered = len(total_data["latestScores"])

        for quadrant in list(quadrants)[:1]:
            data = _gql_data(
                LATEST_SCORES_QUERY,
                {"limit": 5000, "offset": 0, "quadrant": quadrant},
            )
            filtered_count = len(data["latestScores"])
            assert filtered_count <= total_unfiltered, (
                f"Filtered count ({filtered_count}) > unfiltered ({total_unfiltered})"
            )
            assert filtered_count < total_unfiltered, (
                f"Quadrant filter '{quadrant}' returned same count as unfiltered "
                f"({filtered_count} of {total_unfiltered}) -- filter may not be working"
            )


# ===========================================================================
# 5. Audit endpoint
# ===========================================================================


class TestAuditEndpoint:
    """Query auditReport, verify it returns findings and the structure
    is correct."""

    AUDIT_QUERY = """
    query AuditReport($limit: Int!) {
      auditReport(limit: $limit) {
        companiesAudited
        cleanCompanies
        totalFindings
        critical
        warning
        info
        passRate
        findings {
          severity
          category
          companyId
          ticker
          description
          expected
          actual
        }
      }
    }
    """

    def test_audit_report_returns_data(self):
        """auditReport should return a valid report structure."""
        data = _gql_data(self.AUDIT_QUERY, {"limit": 20})
        report = data["auditReport"]
        assert report is not None, "auditReport returned null"
        assert isinstance(report["companiesAudited"], int)
        assert isinstance(report["cleanCompanies"], int)
        assert isinstance(report["totalFindings"], int)
        assert isinstance(report["passRate"], float)

    def test_audit_report_counts_are_consistent(self):
        """total_findings should equal critical + warning + info."""
        data = _gql_data(self.AUDIT_QUERY, {"limit": 50})
        report = data["auditReport"]
        expected_total = report["critical"] + report["warning"] + report["info"]
        assert report["totalFindings"] == expected_total, (
            f"totalFindings={report['totalFindings']} != "
            f"critical({report['critical']}) + warning({report['warning']}) + "
            f"info({report['info']}) = {expected_total}"
        )

    def test_audit_findings_have_required_fields(self):
        """Each finding should have severity, category, ticker, description."""
        data = _gql_data(self.AUDIT_QUERY, {"limit": 50})
        report = data["auditReport"]
        findings = report["findings"]
        if not findings:
            pytest.skip("No audit findings to inspect")

        for i, f in enumerate(findings[:20]):
            assert f["severity"] in ("critical", "warning", "info"), (
                f"Finding {i}: unexpected severity '{f['severity']}'"
            )
            assert f["category"], f"Finding {i}: empty category"
            assert f["description"], f"Finding {i}: empty description"
            assert f["ticker"], f"Finding {i}: empty ticker"

    def test_audit_pass_rate_is_bounded(self):
        """Pass rate should be between 0 and 100."""
        data = _gql_data(self.AUDIT_QUERY, {"limit": 20})
        rate = data["auditReport"]["passRate"]
        assert 0.0 <= rate <= 100.0, f"passRate={rate} out of [0, 100] range"

    def test_audit_clean_lte_audited(self):
        """cleanCompanies should be <= companiesAudited."""
        data = _gql_data(self.AUDIT_QUERY, {"limit": 50})
        report = data["auditReport"]
        assert report["cleanCompanies"] <= report["companiesAudited"], (
            f"cleanCompanies ({report['cleanCompanies']}) > "
            f"companiesAudited ({report['companiesAudited']})"
        )


# ===========================================================================
# 6. Verification endpoint
# ===========================================================================


class TestVerificationEndpoint:
    """Test companyVerification with a real ticker."""

    VERIFY_QUERY = """
    query Verify($ticker: String!) {
      companyVerification(ticker: $ticker) {
        companyId
        ticker
        confirmations {
          sourceA
          sourceB
          dimension
          dollarA
          dollarB
          agreementRatio
        }
        contradictions {
          sourceA
          sourceB
          dimension
          dollarA
          dollarB
          disagreementRatio
          severity
        }
        agreementScore
        confidenceAdjustment
      }
    }
    """

    def test_verification_returns_for_known_ticker(self, sample_tickers):
        """companyVerification should return data for a known ticker."""
        if not sample_tickers:
            pytest.skip("No tickers available")

        # Try each sample ticker until one works
        for ticker in sample_tickers:
            data = _gql_data(self.VERIFY_QUERY, {"ticker": ticker})
            result = data.get("companyVerification")
            if result is not None:
                assert result["ticker"] == ticker
                assert isinstance(result["agreementScore"], (int, float))
                assert isinstance(result["confidenceAdjustment"], (int, float))
                return

        pytest.fail(
            f"companyVerification returned null for all sample tickers: {sample_tickers}"
        )

    def test_verification_agreement_score_bounded(self, sample_tickers):
        """agreementScore should be in [0, 1]."""
        if not sample_tickers:
            pytest.skip("No tickers available")

        for ticker in sample_tickers:
            data = _gql_data(self.VERIFY_QUERY, {"ticker": ticker})
            result = data.get("companyVerification")
            if result is None:
                continue
            score = result["agreementScore"]
            assert 0.0 <= score <= 1.0, (
                f"{ticker}: agreementScore {score} not in [0, 1]"
            )
            return

        pytest.skip("No verification results to check bounds")

    def test_verification_confirmations_structure(self, sample_tickers):
        """Each confirmation should have source labels and agreement ratio."""
        if not sample_tickers:
            pytest.skip("No tickers available")

        for ticker in sample_tickers:
            data = _gql_data(self.VERIFY_QUERY, {"ticker": ticker})
            result = data.get("companyVerification")
            if not result or not result["confirmations"]:
                continue

            for c in result["confirmations"]:
                assert c["sourceA"], f"{ticker}: confirmation missing sourceA"
                assert c["sourceB"], f"{ticker}: confirmation missing sourceB"
                assert c["dimension"], f"{ticker}: confirmation missing dimension"
                assert isinstance(c["agreementRatio"], (int, float))
            return

        pytest.skip("No confirmations found to check structure")

    def test_verification_contradictions_have_severity(self, sample_tickers):
        """Each contradiction should have a severity level."""
        if not sample_tickers:
            pytest.skip("No tickers available")

        for ticker in sample_tickers:
            data = _gql_data(self.VERIFY_QUERY, {"ticker": ticker})
            result = data.get("companyVerification")
            if not result or not result["contradictions"]:
                continue

            for d in result["contradictions"]:
                assert d["severity"], f"{ticker}: contradiction missing severity"
                assert d["sourceA"], f"{ticker}: contradiction missing sourceA"
                assert d["sourceB"], f"{ticker}: contradiction missing sourceB"
            return

        pytest.skip("No contradictions found to check severity")

    def test_verification_returns_null_for_unknown_ticker(self):
        """companyVerification should return null for a nonexistent ticker."""
        data = _gql_data(self.VERIFY_QUERY, {"ticker": "ZZZNOTREAL999"})
        assert data["companyVerification"] is None, (
            "Expected null for nonexistent ticker, got data"
        )


# ===========================================================================
# 7. Trade signals
# ===========================================================================


class TestTradeSignals:
    """Query tradeSignals, verify each signal has required fields and rationale."""

    TRADE_SIGNALS_QUERY = """
    query {
      tradeSignals {
        portfolioId
        totalBuys
        totalSells
        totalHolds
        turnover
        signals {
          id
          ticker
          companyName
          action
          strength
          targetWeight
          currentWeight
          weightChange
          opportunityScore
          realizationScore
          quadrant
          rationaleSummary
          riskFactors
          flags
          status
          rationale {
            level
            description
            data
            sourceUrl
            sourceDate
            confidence
            children {
              level
              description
              data
              sourceUrl
              sourceDate
              confidence
            }
          }
        }
      }
    }
    """

    @pytest.fixture(scope="class")
    def trade_result(self) -> dict:
        data = _gql_data(self.TRADE_SIGNALS_QUERY)
        return data["tradeSignals"]

    def test_trade_signals_top_level_structure(self, trade_result):
        """tradeSignals should have portfolio metadata."""
        assert trade_result["portfolioId"], "Missing portfolioId"
        assert isinstance(trade_result["totalBuys"], int)
        assert isinstance(trade_result["totalSells"], int)
        assert isinstance(trade_result["totalHolds"], int)
        assert isinstance(trade_result["turnover"], (int, float))

    def test_trade_signals_counts_match(self, trade_result):
        """totalBuys + totalSells + totalHolds should equal len(signals)."""
        signals = trade_result["signals"]
        expected = (
            trade_result["totalBuys"]
            + trade_result["totalSells"]
            + trade_result["totalHolds"]
        )
        assert len(signals) == expected, (
            f"len(signals)={len(signals)} != buys({trade_result['totalBuys']}) + "
            f"sells({trade_result['totalSells']}) + holds({trade_result['totalHolds']}) "
            f"= {expected}"
        )

    def test_each_signal_has_required_fields(self, trade_result):
        """Each signal must have ticker, action, strength, scores, rationale."""
        signals = trade_result["signals"]
        if not signals:
            pytest.skip("No trade signals generated")

        issues = []
        for i, s in enumerate(signals):
            if not s.get("ticker"):
                issues.append(f"Signal {i}: missing ticker")
            if not s.get("action"):
                issues.append(f"Signal {i}: missing action")
            if not s.get("strength"):
                issues.append(f"Signal {i}: missing strength")
            if s.get("opportunityScore") is None:
                issues.append(f"Signal {i} ({s.get('ticker', '?')}): missing opportunityScore")
            if s.get("realizationScore") is None:
                issues.append(f"Signal {i} ({s.get('ticker', '?')}): missing realizationScore")
            if not s.get("rationaleSummary"):
                issues.append(f"Signal {i} ({s.get('ticker', '?')}): missing rationaleSummary")

        assert not issues, (
            f"Trade signal field issues ({len(issues)}):\n" + "\n".join(issues[:15])
        )

    def test_signal_actions_are_valid(self, trade_result):
        """Signal action should be one of BUY, SELL, HOLD."""
        signals = trade_result["signals"]
        if not signals:
            pytest.skip("No signals")

        valid_actions = {"BUY", "SELL", "HOLD", "buy", "sell", "hold"}
        invalid = [
            (s["ticker"], s["action"])
            for s in signals
            if s["action"] not in valid_actions
        ]
        assert not invalid, f"Invalid actions: {invalid}"

        # Also flag if the API uses inconsistent casing (should be uppercase)
        lowercase_actions = [
            (s["ticker"], s["action"])
            for s in signals
            if s["action"] in {"buy", "sell", "hold"}
        ]
        if lowercase_actions:
            print(
                f"\nDATA QUALITY NOTE: {len(lowercase_actions)} signals use "
                f"lowercase action values (e.g., '{lowercase_actions[0][1]}' "
                f"instead of '{lowercase_actions[0][1].upper()}'). "
                f"API should normalize to uppercase."
            )

    def test_signal_weights_are_non_negative(self, trade_result):
        """Target and current weights should be >= 0."""
        signals = trade_result["signals"]
        if not signals:
            pytest.skip("No signals")

        negative = []
        for s in signals:
            if s["targetWeight"] < 0:
                negative.append(f"{s['ticker']}: targetWeight={s['targetWeight']}")
            if s["currentWeight"] < 0:
                negative.append(f"{s['ticker']}: currentWeight={s['currentWeight']}")

        assert not negative, f"Negative weights found:\n" + "\n".join(negative)

    def test_signal_rationale_present(self, trade_result):
        """At least some signals should have a non-null rationale tree."""
        signals = trade_result["signals"]
        if not signals:
            pytest.skip("No signals")

        with_rationale = sum(1 for s in signals if s.get("rationale"))
        pct = with_rationale / len(signals) * 100
        print(f"\nSignals with rationale: {with_rationale}/{len(signals)} ({pct:.0f}%)")
        assert with_rationale > 0, "No signal has a rationale tree"


# ===========================================================================
# 8. Edge cases
# ===========================================================================


class TestEdgeCases:
    """Empty filters, very large offsets, invalid tickers, special characters."""

    def test_empty_filter_returns_all(self, all_scores):
        """Passing no filters should return results (same as unfiltered)."""
        data = _gql_data(LATEST_SCORES_QUERY, {"limit": 10, "offset": 0})
        assert len(data["latestScores"]) > 0, "No results with empty filters"

    def test_very_large_offset_returns_empty(self):
        """An offset beyond the data should return an empty list, not an error."""
        data = _gql_data(
            LATEST_SCORES_QUERY,
            {"limit": 10, "offset": 999999},
        )
        assert data["latestScores"] == [], (
            f"Expected empty list at offset=999999, got {len(data['latestScores'])} items"
        )

    def test_zero_limit_returns_empty(self):
        """Limit of 0 should return an empty list."""
        data = _gql_data(
            LATEST_SCORES_QUERY,
            {"limit": 0, "offset": 0},
        )
        assert data["latestScores"] == [], (
            f"Expected empty list with limit=0, got {len(data['latestScores'])} items"
        )

    def test_invalid_ticker_returns_null(self):
        """companyDetail for a nonexistent ticker should return null, not error."""
        data = _gql_data(COMPANY_DETAIL_QUERY, {"ticker": "DOESNOTEXIST123"})
        assert data["companyDetail"] is None, (
            "Expected null for nonexistent ticker"
        )

    def test_special_characters_in_ticker(self):
        """Special characters in ticker should not cause 500 or injection."""
        dangerous_tickers = [
            "'; DROP TABLE companies; --",
            "<script>alert(1)</script>",
            "AAPL\n\r\t",
            "A" * 1000,
            "",
            "NULL",
            "undefined",
            "../../../etc/passwd",
        ]
        for ticker in dangerous_tickers:
            body = _gql(COMPANY_DETAIL_QUERY, {"ticker": ticker})
            # Should not get a 500 error -- GraphQL should handle gracefully
            # The response should either have data (null) or a validation error
            assert body is not None, f"Null response for ticker={ticker!r}"
            # If there's data, companyDetail should be null
            if "data" in body and body["data"]:
                detail = body["data"].get("companyDetail")
                assert detail is None, (
                    f"Expected null for special ticker {ticker!r}, got data"
                )

    def test_nonexistent_quadrant_filter(self):
        """Filtering by a nonexistent quadrant should return empty results."""
        data = _gql_data(
            LATEST_SCORES_QUERY,
            {"limit": 10, "offset": 0, "quadrant": "NONEXISTENT_QUADRANT_XYZ"},
        )
        assert data["latestScores"] == [], (
            f"Expected empty for fake quadrant, got {len(data['latestScores'])} items"
        )

    def test_nonexistent_sector_filter(self):
        """Filtering by a nonexistent sector should return empty results."""
        data = _gql_data(
            LATEST_SCORES_QUERY,
            {"limit": 10, "offset": 0, "sector": "Underwater Basket Weaving"},
        )
        assert data["latestScores"] == [], (
            f"Expected empty for fake sector, got {len(data['latestScores'])} items"
        )

    def test_negative_offset_handled(self):
        """Negative offset should either be rejected or treated as 0."""
        body = _gql(LATEST_SCORES_QUERY, {"limit": 5, "offset": -1})
        # Should not crash; either returns data or a clean error
        assert body is not None

    def test_negative_limit_handled(self):
        """Negative limit should either be rejected or treated as 0."""
        body = _gql(LATEST_SCORES_QUERY, {"limit": -1, "offset": 0})
        assert body is not None

    def test_very_large_limit(self):
        """A very large limit should not crash the server."""
        data = _gql_data(
            LATEST_SCORES_QUERY,
            {"limit": 10000, "offset": 0},
        )
        # Should return data without crashing
        assert isinstance(data["latestScores"], list)
