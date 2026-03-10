"""Adversarial tests for the GraphQL API at http://localhost:8080/graphql.

These tests probe edge cases, injection attacks, malformed inputs, and data
integrity to surface bugs and missing hardening in the API layer.

Run with:
    /opt/homebrew/bin/python3.11 -m pytest tests/adversarial/test_api_hardening.py -v
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import httpx
import pytest

BASE_URL = "http://localhost:8080"
GQL_URL = f"{BASE_URL}/graphql"
TIMEOUT = 30.0

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def gql(query: str, variables: dict | None = None) -> httpx.Response:
    """Send a GraphQL POST request and return the raw response."""
    payload: dict[str, Any] = {"query": query}
    if variables:
        payload["variables"] = variables
    return httpx.post(
        GQL_URL,
        json=payload,
        timeout=TIMEOUT,
    )


def gql_ok(query: str, variables: dict | None = None) -> dict:
    """Send a GraphQL query, assert 200, return the JSON body."""
    r = gql(query, variables)
    assert r.status_code == 200, f"Expected 200, got {r.status_code}: {r.text[:500]}"
    body = r.json()
    assert "data" in body, f"Missing 'data' key in response: {body}"
    return body


def assert_no_500(r: httpx.Response):
    """No query -- valid or invalid -- should produce a 500."""
    assert r.status_code != 500, (
        f"Server returned 500 Internal Server Error: {r.text[:1000]}"
    )


# =========================================================================
# 1. Edge-case inputs
# =========================================================================

class TestEdgeCaseInputs:
    """Boundary and adversarial input values."""

    def test_empty_string_ticker_company_detail(self):
        """companyDetail(ticker: '') should return null, not crash."""
        body = gql_ok('{ companyDetail(ticker: "") { company { ticker } } }')
        # Either null or an empty result -- never a 500
        assert body["data"]["companyDetail"] is None or isinstance(
            body["data"]["companyDetail"], dict
        )

    def test_nonexistent_ticker_company_detail(self):
        """companyDetail for a garbage ticker should be null."""
        body = gql_ok('{ companyDetail(ticker: "ZZZZZ") { company { ticker } } }')
        assert body["data"]["companyDetail"] is None

    def test_empty_string_ticker_evidence(self):
        """evidenceForCompany(ticker: '') should return empty list."""
        body = gql_ok('{ evidenceForCompany(ticker: "") { id } }')
        assert body["data"]["evidenceForCompany"] == []

    def test_nonexistent_ticker_evidence(self):
        """evidenceForCompany for unknown ticker returns empty list."""
        body = gql_ok('{ evidenceForCompany(ticker: "ZZZZZ") { id } }')
        assert body["data"]["evidenceForCompany"] == []

    def test_empty_ticker_verification(self):
        """companyVerification(ticker: '') should return null."""
        body = gql_ok('{ companyVerification(ticker: "") { ticker } }')
        assert body["data"]["companyVerification"] is None

    def test_nonexistent_ticker_verification(self):
        """companyVerification for unknown ticker returns null."""
        body = gql_ok('{ companyVerification(ticker: "ZZZZZ") { ticker } }')
        assert body["data"]["companyVerification"] is None

    # -- SQL injection --------------------------------------------------------

    def test_sql_injection_in_search(self):
        """SQL injection via search should not crash the server."""
        r = gql('{ companies(search: "\'; DROP TABLE companies; --") { ticker } }')
        assert_no_500(r)
        # If it returns 200 the query was safely parameterised
        if r.status_code == 200:
            body = r.json()
            # Should either have data or graphql errors -- not a raw DB error
            assert "data" in body or "errors" in body

    def test_sql_injection_in_sector(self):
        r = gql('{ companies(sector: "\'; DROP TABLE companies; --") { ticker } }')
        assert_no_500(r)

    def test_sql_injection_in_ticker(self):
        r = gql('{ companyDetail(ticker: "\'; DROP TABLE companies; --") { company { ticker } } }')
        assert_no_500(r)

    # -- XSS ------------------------------------------------------------------

    def test_xss_in_search(self):
        """XSS payload in search should not be reflected in output unsanitised."""
        body = gql_ok('{ companies(search: "<script>alert(1)</script>") { companyName } }')
        companies = body["data"]["companies"]
        for c in companies:
            if c["companyName"]:
                assert "<script>" not in c["companyName"]

    # -- Limit / offset edge cases --------------------------------------------

    def test_negative_limit(self):
        """Negative limit should not crash (ideally returns error or empty)."""
        r = gql("{ latestScores(limit: -1) { id } }")
        assert_no_500(r)

    def test_zero_limit(self):
        """Zero limit should return empty list."""
        body = gql_ok("{ latestScores(limit: 0) { id } }")
        scores = body["data"]["latestScores"]
        assert isinstance(scores, list)
        # Zero limit should logically return 0 rows
        assert len(scores) == 0, f"Expected 0 scores with limit=0, got {len(scores)}"

    def test_huge_limit(self):
        """Very large limit should not OOM the server."""
        r = gql("{ latestScores(limit: 999999) { id opportunity } }")
        assert_no_500(r)
        if r.status_code == 200:
            body = r.json()
            assert "data" in body

    def test_huge_offset(self):
        """Huge offset returns empty list."""
        body = gql_ok("{ latestScores(offset: 999999) { id } }")
        assert body["data"]["latestScores"] == []

    def test_negative_offset(self):
        """Negative offset should not crash."""
        r = gql("{ latestScores(offset: -1) { id } }")
        assert_no_500(r)

    def test_invalid_quadrant_filter(self):
        """Filtering by a non-existent quadrant should return empty, not crash."""
        r = gql('{ latestScores(quadrant: "fake_quadrant") { id } }')
        assert_no_500(r)
        if r.status_code == 200:
            body = r.json()
            scores = body["data"]["latestScores"]
            assert isinstance(scores, list)

    def test_unicode_search(self):
        """Unicode characters in search should be handled gracefully."""
        r = gql('{ companies(search: "\u5fae\u8f6f") { ticker } }')
        assert_no_500(r)
        if r.status_code == 200:
            body = r.json()
            assert isinstance(body["data"]["companies"], list)

    def test_very_long_search(self):
        """10k-char search should not crash or hang."""
        long_str = "A" * 10000
        r = gql(f'{{ companies(search: "{long_str}") {{ ticker }} }}')
        assert_no_500(r)

    def test_null_bytes_in_search(self):
        """Null bytes in input should not crash."""
        r = gql('{ companies(search: "test\\u0000evil") { ticker } }')
        assert_no_500(r)

    def test_negative_evidence_limit(self):
        """evidenceForCompany with negative limit."""
        r = gql('{ evidenceForCompany(ticker: "AAPL", limit: -1) { id } }')
        assert_no_500(r)

    def test_zero_evidence_limit(self):
        """evidenceForCompany with limit=0 should return empty."""
        body = gql_ok('{ evidenceForCompany(ticker: "AAPL", limit: 0) { id } }')
        evidence = body["data"]["evidenceForCompany"]
        assert isinstance(evidence, list)
        assert len(evidence) == 0, f"Expected 0 evidence with limit=0, got {len(evidence)}"

    def test_negative_stale_limit(self):
        """staleScores with negative limit."""
        r = gql("{ staleScores(limit: -1) { id } }")
        assert_no_500(r)


# =========================================================================
# 2. API robustness
# =========================================================================

class TestAPIRobustness:
    """Protocol-level robustness tests."""

    def test_malformed_query_missing_brace(self):
        """Missing closing brace should return a parse error, not 500."""
        r = gql("{ latestScores { id }")
        assert_no_500(r)
        if r.status_code == 200:
            body = r.json()
            assert "errors" in body, "Malformed query should produce errors"

    def test_nonexistent_field(self):
        """Requesting a field that does not exist should produce a validation error."""
        r = gql("{ latestScores { id doesNotExistField } }")
        assert_no_500(r)
        if r.status_code == 200:
            body = r.json()
            assert "errors" in body

    def test_empty_body(self):
        """POST with empty body should not produce 500."""
        r = httpx.post(GQL_URL, content=b"", headers={"Content-Type": "application/json"}, timeout=TIMEOUT)
        assert_no_500(r)

    def test_get_request(self):
        """GET request to graphql endpoint should not 500."""
        r = httpx.get(GQL_URL, timeout=TIMEOUT)
        assert_no_500(r)

    def test_wrong_content_type(self):
        """Sending text/plain instead of application/json."""
        r = httpx.post(
            GQL_URL,
            content='{ "query": "{ latestScores { id } }" }',
            headers={"Content-Type": "text/plain"},
            timeout=TIMEOUT,
        )
        assert_no_500(r)

    def test_invalid_json_body(self):
        """Completely invalid JSON in POST body."""
        r = httpx.post(
            GQL_URL,
            content=b"this is not json",
            headers={"Content-Type": "application/json"},
            timeout=TIMEOUT,
        )
        assert_no_500(r)

    def test_missing_query_key(self):
        """JSON body without a 'query' key."""
        r = httpx.post(
            GQL_URL,
            json={"notquery": "{ latestScores { id } }"},
            timeout=TIMEOUT,
        )
        assert_no_500(r)

    def test_query_is_null(self):
        """query key set to null."""
        r = httpx.post(GQL_URL, json={"query": None}, timeout=TIMEOUT)
        assert_no_500(r)

    def test_query_is_number(self):
        """query key set to a number."""
        r = httpx.post(GQL_URL, json={"query": 42}, timeout=TIMEOUT)
        assert_no_500(r)

    def test_introspection(self):
        """Introspection query should work (or be disabled intentionally)."""
        body = gql_ok("{ __schema { queryType { name } } }")
        assert body["data"]["__schema"]["queryType"]["name"] == "Query"


# =========================================================================
# 3. Data integrity
# =========================================================================

class TestDataIntegrity:
    """Verify that returned data is consistent and well-formed."""

    @pytest.fixture(scope="class")
    def scores(self) -> list[dict]:
        body = gql_ok("""
        {
            latestScores(limit: 20) {
                id companyId opportunity realization
                quadrant quadrantLabel flags scoredAt
                scoreAgeDays stalenessLevel
                combinedRank aiIndexUsd captureProbability
                opportunityUsd evidenceDollars
                agreementScore numConfirmations numContradictions
            }
        }
        """)
        return body["data"]["latestScores"]

    def test_scores_returned(self, scores):
        """At least some scores should exist in the DB."""
        assert len(scores) > 0, "No scores returned -- is the database seeded?"

    def test_opportunity_range(self, scores):
        """opportunity should be in [0, 1]."""
        for s in scores:
            assert 0.0 <= s["opportunity"] <= 1.0, (
                f"Score id={s['id']}: opportunity={s['opportunity']} outside [0,1]"
            )

    def test_realization_range(self, scores):
        """realization should be in [0, 1]."""
        for s in scores:
            assert 0.0 <= s["realization"] <= 1.0, (
                f"Score id={s['id']}: realization={s['realization']} outside [0,1]"
            )

    def test_scored_at_parseable_and_in_past(self, scores):
        """scored_at should be a valid ISO datetime in the past."""
        now = datetime.now(timezone.utc)
        for s in scores:
            dt_str = s["scoredAt"]
            assert dt_str is not None, f"Score id={s['id']} has null scoredAt"
            dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
            # If the API returns a naive datetime, treat it as UTC for comparison
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            assert dt <= now, f"Score id={s['id']}: scoredAt {dt} is in the future"

    VALID_QUADRANTS = {
        "high_opp_high_real",
        "high_opp_low_real",
        "low_opp_high_real",
        "low_opp_low_real",
        None,
    }

    def test_quadrant_values(self, scores):
        """Quadrant should be one of the expected values."""
        for s in scores:
            assert s["quadrant"] in self.VALID_QUADRANTS, (
                f"Score id={s['id']}: unexpected quadrant '{s['quadrant']}'"
            )

    def test_flags_are_list_of_strings(self, scores):
        for s in scores:
            flags = s["flags"]
            assert isinstance(flags, list), f"Score id={s['id']}: flags is not a list"
            for f in flags:
                assert isinstance(f, str), f"Score id={s['id']}: flag {f!r} is not a string"

    def test_score_age_days_matches_scored_at(self, scores):
        """score_age_days should approximately match now - scored_at."""
        now = datetime.now(timezone.utc)
        for s in scores:
            if s["scoreAgeDays"] is None:
                continue
            dt = datetime.fromisoformat(s["scoredAt"].replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            expected_age = (now - dt).days
            actual_age = s["scoreAgeDays"]
            # Allow 1 day tolerance for timezone / boundary effects
            assert abs(actual_age - expected_age) <= 1, (
                f"Score id={s['id']}: scoreAgeDays={actual_age} but expected ~{expected_age}"
            )

    def test_staleness_level_consistent(self, scores):
        """staleness_level should be one of fresh/warning/critical and match age."""
        for s in scores:
            level = s["stalenessLevel"]
            if level is None:
                continue
            assert level in ("fresh", "warning", "critical"), (
                f"Score id={s['id']}: unexpected stalenessLevel '{level}'"
            )

    def test_company_detail_consistency(self, scores):
        """Pick a real ticker from scores and verify companyDetail returns matching data."""
        if not scores:
            pytest.skip("No scores to cross-reference")

        # Get a ticker from companies list using company_id from first score
        company_id = scores[0]["companyId"]
        companies_body = gql_ok("{ companies(limit: 200) { id ticker } }")
        ticker = None
        for c in companies_body["data"]["companies"]:
            if c["id"] == company_id:
                ticker = c["ticker"]
                break

        if not ticker:
            pytest.skip(f"Could not find ticker for companyId={company_id}")

        detail_body = gql_ok(f"""
        {{
            companyDetail(ticker: "{ticker}") {{
                company {{ id ticker }}
                latestScore {{ id opportunity realization }}
            }}
        }}
        """)
        detail = detail_body["data"]["companyDetail"]
        assert detail is not None, f"companyDetail returned null for known ticker {ticker}"
        assert detail["company"]["ticker"] == ticker


# =========================================================================
# 4. All queries smoke test (no 500s)
# =========================================================================

class TestAllQueriesSmokeTest:
    """Hit every query at least once and verify no 500 errors."""

    def test_companies_basic(self):
        body = gql_ok("{ companies(limit: 5) { id ticker companyName sector } }")
        assert isinstance(body["data"]["companies"], list)

    def test_company_detail_with_all_nested(self):
        """Fetch a real ticker and request all nested fields."""
        # First get a ticker
        companies = gql_ok("{ companies(limit: 1) { ticker } }")["data"]["companies"]
        if not companies:
            pytest.skip("No companies in DB")
        ticker = companies[0]["ticker"]

        body = gql_ok(f"""
        {{
            companyDetail(ticker: "{ticker}") {{
                company {{ id ticker companyName exchange sector industry sic naics isActive slug githubUrl careersUrl irUrl blogUrl }}
                latestScore {{ id opportunity realization quadrant quadrantLabel flags scoredAt scoreAgeDays stalenessLevel combinedRank aiIndexUsd captureProbability opportunityUsd evidenceDollars costOppScore revenueOppScore compositeOppScore costCaptureScore revenueCaptureScore filingNlpScore productScore githubScore analystScore costRoi revenueRoi agreementScore numConfirmations numContradictions }}
                evidence {{ id companyId evidenceType evidenceSubtype sourceName sourceUrl sourceDate sourceExcerpt targetDimension captureStage signalStrength dollarEstimateUsd observedAt }}
                financials {{ id companyId metric value valueUnits sourceName fiscalPeriod sourceDatetime }}
                peers {{ id ticker companyName }}
                evidenceGroups {{ id companyId targetDimension evidenceType passageCount representativeText meanConfidence dateEarliest dateLatest }}
                valuations {{ id groupId stage evidenceType narrative confidence dollarLow dollarMid dollarHigh specificity magnitude }}
            }}
        }}
        """)
        assert body["data"]["companyDetail"] is not None

    def test_latest_scores(self):
        body = gql_ok("{ latestScores(limit: 5) { id opportunity realization } }")
        assert isinstance(body["data"]["latestScores"], list)

    def test_latest_scores_with_sector_filter(self):
        body = gql_ok('{ latestScores(limit: 5, sector: "Technology") { id } }')
        assert isinstance(body["data"]["latestScores"], list)

    def test_latest_scores_with_quadrant_filter(self):
        body = gql_ok('{ latestScores(limit: 5, quadrant: "high_opp_high_real") { id quadrant } }')
        for s in body["data"]["latestScores"]:
            assert s["quadrant"] == "high_opp_high_real"

    def test_pipeline_status(self):
        body = gql_ok("""
        {
            pipelineStatus {
                totalCompanies companiesScored totalEvidence
                lastRun { id runId task subtask runType status tickersSucceeded tickersFailed startedAt completedAt errorMessage }
                recentRuns { id runId }
            }
        }
        """)
        ps = body["data"]["pipelineStatus"]
        assert ps["totalCompanies"] >= 0
        assert ps["companiesScored"] >= 0
        assert ps["totalEvidence"] >= 0

    def test_trade_signals(self):
        r = gql("""
        {
            tradeSignals {
                portfolioId totalBuys totalSells totalHolds turnover
                signals { id ticker action strength targetWeight currentWeight weightChange opportunityScore realizationScore quadrant rationaleSummary riskFactors flags status }
            }
        }
        """)
        assert_no_500(r)
        # tradeSignals may fail if trading module has issues -- that's a separate concern
        # but it should never be a 500

    def test_fact_graph_stats(self):
        body = gql_ok("""
        {
            factGraphStats {
                totalNodes totalEdges totalAttributes
                missingValues lowConfidenceValues counterfactualBranches
                completenessPct nodesByType
            }
        }
        """)
        fgs = body["data"]["factGraphStats"]
        assert fgs["totalNodes"] >= 0
        assert 0.0 <= fgs["completenessPct"] <= 100.0

    def test_changelog(self):
        body = gql_ok("""
        {
            changelog {
                version title date summary status
                changes { description changeType component }
            }
        }
        """)
        assert isinstance(body["data"]["changelog"], list)

    def test_evidence_for_company(self):
        companies = gql_ok("{ companies(limit: 1) { ticker } }")["data"]["companies"]
        if not companies:
            pytest.skip("No companies")
        ticker = companies[0]["ticker"]
        body = gql_ok(f"""
        {{
            evidenceForCompany(ticker: "{ticker}", limit: 5) {{
                id companyId evidenceType sourceName sourceUrl
            }}
        }}
        """)
        assert isinstance(body["data"]["evidenceForCompany"], list)

    def test_stale_scores(self):
        """BUG: staleScores resolver has a timezone mismatch -- it creates a tz-aware
        cutoff datetime but the DB stores naive datetimes, causing a DataError."""
        r = gql("{ staleScores(limit: 5) { id scoredAt scoreAgeDays stalenessLevel } }")
        assert_no_500(r)
        body = r.json()
        # The query should succeed with data, not return errors
        assert body.get("data") is not None and body["data"].get("staleScores") is not None, (
            f"staleScores query failed (likely timezone bug in resolver): "
            f"{[e['message'][:120] for e in body.get('errors', [])]}"
        )
        assert isinstance(body["data"]["staleScores"], list)

    def test_company_verification(self):
        companies = gql_ok("{ companies(limit: 1) { ticker } }")["data"]["companies"]
        if not companies:
            pytest.skip("No companies")
        ticker = companies[0]["ticker"]
        r = gql(f"""
        {{
            companyVerification(ticker: "{ticker}") {{
                companyId ticker agreementScore confidenceAdjustment
                confirmations {{ sourceA sourceB dimension dollarA dollarB agreementRatio }}
                contradictions {{ sourceA sourceB dimension dollarA dollarB disagreementRatio severity }}
            }}
        }}
        """)
        assert_no_500(r)


# =========================================================================
# 5. Concurrent / stress
# =========================================================================

class TestConcurrent:
    """Light concurrency tests."""

    def test_parallel_queries(self):
        """Fire several queries concurrently -- none should 500."""
        import asyncio

        async def _run():
            async with httpx.AsyncClient(timeout=TIMEOUT) as client:
                queries = [
                    '{ companies(limit: 3) { ticker } }',
                    '{ latestScores(limit: 3) { id } }',
                    '{ pipelineStatus { totalCompanies } }',
                    '{ changelog { version } }',
                    '{ factGraphStats { totalNodes } }',
                ]
                tasks = [
                    client.post(GQL_URL, json={"query": q})
                    for q in queries
                ]
                responses = await asyncio.gather(*tasks)
                for r in responses:
                    assert_no_500(r)

        asyncio.run(_run())


# =========================================================================
# 6. Pagination
# =========================================================================

class TestPagination:
    """Verify pagination works correctly."""

    def test_latest_scores_ids_are_real(self):
        """BUG: latestScores returns id=0 for every score because the materialized
        view / dict doesn't include the score id. This makes pagination verification
        impossible and breaks client caching."""
        page = gql_ok("{ latestScores(limit: 5, offset: 0) { id companyId } }")["data"]["latestScores"]
        if not page:
            pytest.skip("No scores")
        ids = [s["id"] for s in page]
        # All ids being 0 indicates the id field is not populated from the query
        assert not all(i == 0 for i in ids), (
            f"All latestScores have id=0 -- the score id is not being returned from "
            f"the underlying query/view. IDs: {ids}"
        )

    def test_pagination_consistency(self):
        """Page 1 + Page 2 should not overlap (uses companyId since id is bugged)."""
        page1 = gql_ok("{ latestScores(limit: 5, offset: 0) { id companyId } }")["data"]["latestScores"]
        page2 = gql_ok("{ latestScores(limit: 5, offset: 5) { id companyId } }")["data"]["latestScores"]

        cids1 = {s["companyId"] for s in page1}
        cids2 = {s["companyId"] for s in page2}
        overlap = cids1 & cids2
        assert not overlap, f"Pagination overlap on companyId: {overlap}"

    def test_offset_zero_equals_no_offset(self):
        """offset=0 should be same as omitting offset."""
        with_offset = gql_ok("{ latestScores(limit: 5, offset: 0) { id } }")["data"]["latestScores"]
        without_offset = gql_ok("{ latestScores(limit: 5) { id } }")["data"]["latestScores"]
        assert [s["id"] for s in with_offset] == [s["id"] for s in without_offset]

    def test_companies_pagination(self):
        """Companies pagination should also not overlap."""
        page1 = gql_ok("{ companies(limit: 3, offset: 0) { id } }")["data"]["companies"]
        page2 = gql_ok("{ companies(limit: 3, offset: 3) { id } }")["data"]["companies"]
        ids1 = {c["id"] for c in page1}
        ids2 = {c["id"] for c in page2}
        assert not (ids1 & ids2), "Company pagination overlap"
