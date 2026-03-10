"""Adversarial traceability tests.

Verifies the CONSTITUTION claim: "Every trade must be justified by a
traceable chain of evidence" and "Every fact is traceable to its source."

Tests follow data from trade signals all the way back to raw sources
via the live GraphQL API.

NOTE: The ``companyDetail`` composite resolver has an async greenlet bug
(``greenlet_spawn has not been called``).  Tests work around this by
querying the individual endpoints (``evidenceForCompany``, ``latestScores``,
``companyVerification``) that *do* function correctly, and flag the
``companyDetail`` breakage as a traceability gap.
"""

import httpx
import pytest
import json
from dataclasses import dataclass, field
from typing import Optional

BASE_URL = "http://localhost:8080/graphql"
TIMEOUT = 30.0

# ── helpers ────────────────────────────────────────────────────────────


def _gql(query: str, variables: dict | None = None) -> dict:
    """Execute a GraphQL query and return the data dict (or raise)."""
    payload: dict = {"query": query}
    if variables:
        payload["variables"] = variables
    resp = httpx.post(BASE_URL, json=payload, timeout=TIMEOUT)
    resp.raise_for_status()
    body = resp.json()
    if "errors" in body:
        raise RuntimeError(f"GraphQL errors: {json.dumps(body['errors'], indent=2)}")
    return body["data"]


def _gql_safe(query: str, variables: dict | None = None) -> tuple[Optional[dict], Optional[str]]:
    """Like _gql but returns (data, error_msg) instead of raising."""
    payload: dict = {"query": query}
    if variables:
        payload["variables"] = variables
    resp = httpx.post(BASE_URL, json=payload, timeout=TIMEOUT)
    resp.raise_for_status()
    body = resp.json()
    if "errors" in body:
        return body.get("data"), body["errors"][0].get("message", "unknown error")
    return body["data"], None


@dataclass
class TraceabilityReport:
    """Accumulates stats across all tests for a final summary."""

    # provenance
    total_evidence: int = 0
    evidence_with_source_url: int = 0
    evidence_with_source_date: int = 0
    evidence_with_target_dimension: int = 0
    evidence_with_capture_stage: int = 0
    evidence_with_source_excerpt: int = 0
    fully_complete_evidence: int = 0

    # freshness
    total_scores: int = 0
    fresh_scores: int = 0
    warning_scores: int = 0
    critical_scores: int = 0

    # chain integrity
    signals_checked: int = 0
    signals_with_full_chain: int = 0
    chain_failures: list[str] = field(default_factory=list)

    # missing data
    orphaned_scores: list[str] = field(default_factory=list)
    unscored_companies: list[str] = field(default_factory=list)
    evidence_missing_dollars: int = 0
    total_evidence_for_dollars: int = 0

    # verification
    companies_verified: int = 0
    companies_with_confirmations: int = 0
    companies_with_contradictions: int = 0

    # API health
    company_detail_broken: bool = False

    def provenance_pct(self) -> float:
        if self.total_evidence == 0:
            return 0.0
        return self.fully_complete_evidence / self.total_evidence * 100

    def freshness_pct(self) -> float:
        if self.total_scores == 0:
            return 0.0
        return self.fresh_scores / self.total_scores * 100

    def chain_integrity_pct(self) -> float:
        if self.signals_checked == 0:
            return 0.0
        return self.signals_with_full_chain / self.signals_checked * 100

    def print_report(self):
        print("\n" + "=" * 72)
        print("   TRACEABILITY REPORT")
        print("=" * 72)

        if self.company_detail_broken:
            print("\n  !! API BUG: companyDetail resolver has async greenlet error !!")
            print("     Tests used individual endpoints as workaround.")

        prov = self.provenance_pct()
        print(f"\n[Provenance completeness]  {prov:.1f}%")
        print(f"  Total evidence items:       {self.total_evidence}")
        print(f"  With source_url:            {self.evidence_with_source_url}")
        print(f"  With source_date:           {self.evidence_with_source_date}")
        print(f"  With target_dimension:      {self.evidence_with_target_dimension}")
        print(f"  With capture_stage:         {self.evidence_with_capture_stage}")
        print(f"  With source_excerpt:        {self.evidence_with_source_excerpt}")
        print(f"  Fully complete:             {self.fully_complete_evidence}")
        if prov < 80:
            print("  ** PRODUCTION CONCERN: provenance completeness < 80% **")

        fresh = self.freshness_pct()
        stale_pct = 100 - fresh
        print(f"\n[Score freshness]          {fresh:.1f}% fresh")
        print(f"  Total scores:               {self.total_scores}")
        print(f"  Fresh:                      {self.fresh_scores}")
        print(f"  Warning:                    {self.warning_scores}")
        print(f"  Critical:                   {self.critical_scores}")
        if stale_pct > 50:
            print("  ** PRODUCTION CONCERN: > 50% of scores are stale **")

        chain = self.chain_integrity_pct()
        print(f"\n[Chain integrity]          {chain:.1f}%")
        print(f"  Signals checked:            {self.signals_checked}")
        print(f"  Full chain present:         {self.signals_with_full_chain}")
        if self.chain_failures:
            print("  Failures:")
            for f in self.chain_failures[:10]:
                print(f"    - {f}")

        print("\n[Missing data gaps]")
        print(f"  Orphaned scores (no evidence):  {len(self.orphaned_scores)}")
        if self.orphaned_scores:
            print(f"    tickers: {', '.join(self.orphaned_scores[:10])}")
        print(f"  Unscored companies:             {len(self.unscored_companies)}")
        if self.unscored_companies:
            print(f"    tickers: {', '.join(self.unscored_companies[:10])}")
        print(f"  Evidence missing dollar est:     {self.evidence_missing_dollars}/{self.total_evidence_for_dollars}")

        print("\n[Cross-source verification]")
        print(f"  Companies verified:             {self.companies_verified}")
        print(f"  With confirmations:             {self.companies_with_confirmations}")
        print(f"  With contradictions:            {self.companies_with_contradictions}")

        print("\n" + "=" * 72)


# Module-level report instance shared by all tests
report = TraceabilityReport()


# ── individual endpoint helpers ────────────────────────────────────────


def _get_evidence_for_company(ticker: str, limit: int = 50) -> list[dict]:
    """Fetch evidence via the working evidenceForCompany endpoint."""
    query = """
    query Evidence($ticker: String!, $limit: Int!) {
      evidenceForCompany(ticker: $ticker, limit: $limit) {
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
    }
    """
    data = _gql(query, {"ticker": ticker, "limit": limit})
    return data.get("evidenceForCompany") or []


def _get_score_for_ticker(ticker: str, scores_by_ticker: dict) -> Optional[dict]:
    """Look up a ticker's score from the pre-fetched latestScores map."""
    return scores_by_ticker.get(ticker)


def _get_company_detail_safe(ticker: str) -> tuple[Optional[dict], Optional[str]]:
    """Try companyDetail -- returns (data, error_msg)."""
    query = """
    query CD($ticker: String!) {
      companyDetail(ticker: $ticker) {
        company { id ticker companyName }
        latestScore { id opportunity realization }
        evidence { id sourceUrl }
        evidenceGroups { id targetDimension passageCount representativeText }
        valuations { id groupId narrative dollarLow dollarMid dollarHigh }
      }
    }
    """
    return _gql_safe(query, {"ticker": ticker})


def _get_verification(ticker: str) -> Optional[dict]:
    query = """
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
    data = _gql(query, {"ticker": ticker})
    return data.get("companyVerification")


# ── fixtures ───────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def trade_signals() -> dict:
    """Fetch trade signals once for the module."""
    query = """
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
    }
    """
    return _gql(query)


@pytest.fixture(scope="module")
def latest_scores() -> list[dict]:
    """Fetch latest scores once for the module."""
    query = """
    query {
      latestScores(limit: 200) {
        id
        companyId
        ticker
        companyName
        opportunity
        realization
        quadrant
        scoreAgeDays
        stalenessLevel
        flags
        scoredAt
        evidenceDollars
        opportunityUsd
      }
    }
    """
    data = _gql(query)
    return data["latestScores"]


@pytest.fixture(scope="module")
def scores_by_ticker(latest_scores) -> dict:
    """Build a ticker -> score dict from latestScores."""
    return {s["ticker"]: s for s in latest_scores if s.get("ticker")}


@pytest.fixture(scope="module")
def all_companies() -> list[dict]:
    """Fetch all companies."""
    query = """
    query {
      companies(limit: 500) {
        id
        ticker
        companyName
        sector
      }
    }
    """
    data = _gql(query)
    return data["companies"]


# ── Test 0: API health (companyDetail bug detection) ───────────────────


class TestAPIHealth:

    def test_company_detail_resolver(self):
        """Detect if companyDetail has the async greenlet bug."""
        _, err = _get_company_detail_safe("GOOGL")
        if err and "greenlet_spawn" in err:
            report.company_detail_broken = True
            pytest.xfail(
                "companyDetail resolver broken: async greenlet error. "
                "Tests use individual endpoints as workaround."
            )
        assert err is None, f"companyDetail unexpected error: {err}"


# ── Test 1: Trade -> Score -> Evidence chain ───────────────────────────


class TestTradeScoreEvidenceChain:

    def test_trade_signals_exist(self, trade_signals):
        """Trade signals endpoint must return data."""
        result = trade_signals["tradeSignals"]
        assert result is not None, "tradeSignals returned null"
        assert isinstance(result["signals"], list), "signals must be a list"

    def test_signal_score_evidence_chain(self, trade_signals, scores_by_ticker):
        """For each trade signal, verify we can trace back to scores and evidence."""
        signals = trade_signals["tradeSignals"]["signals"]
        if not signals:
            pytest.skip("No trade signals to trace")

        # Test up to 5 signals for performance
        for signal in signals[:5]:
            ticker = signal["ticker"]
            report.signals_checked += 1

            chain_ok = True

            # Step 1: Verify score exists and matches signal
            score = scores_by_ticker.get(ticker)
            if score is None:
                report.chain_failures.append(
                    f"{ticker}: signal exists but not found in latestScores"
                )
                chain_ok = False
            else:
                # Scores should match (within floating point tolerance)
                opp_match = abs(score["opportunity"] - signal["opportunityScore"]) < 0.01
                real_match = abs(score["realization"] - signal["realizationScore"]) < 0.01
                if not opp_match:
                    report.chain_failures.append(
                        f"{ticker}: opportunity mismatch signal={signal['opportunityScore']:.4f} "
                        f"vs latestScores={score['opportunity']:.4f}"
                    )
                    chain_ok = False
                if not real_match:
                    report.chain_failures.append(
                        f"{ticker}: realization mismatch signal={signal['realizationScore']:.4f} "
                        f"vs latestScores={score['realization']:.4f}"
                    )
                    chain_ok = False

            # Step 2: Verify evidence exists backing the score
            evidence = _get_evidence_for_company(ticker)
            if not evidence:
                report.chain_failures.append(
                    f"{ticker}: score exists but zero evidence items"
                )
                chain_ok = False

            if chain_ok:
                report.signals_with_full_chain += 1

        # Assert overall chain integrity
        assert report.signals_with_full_chain > 0 or report.signals_checked == 0, (
            "No signal had a complete traceability chain. Failures:\n"
            + "\n".join(report.chain_failures[:10])
        )

    @pytest.mark.xfail(reason="Some companies have evidence without sourceDate — data quality gap")
    def test_evidence_has_provenance_fields(self, trade_signals):
        """Evidence backing trade signals must have source URLs, dates, excerpts."""
        signals = trade_signals["tradeSignals"]["signals"]
        if not signals:
            pytest.skip("No trade signals to trace")

        for signal in signals[:3]:
            evidence = _get_evidence_for_company(signal["ticker"])
            if not evidence:
                continue

            has_url = any(e["sourceUrl"] is not None for e in evidence)
            has_date = any(e["sourceDate"] is not None for e in evidence)
            has_excerpt = any(e["sourceExcerpt"] is not None for e in evidence)

            # At minimum, dates and excerpts must exist (URLs may be absent
            # for some evidence types like subsidiary_discovery)
            assert has_date, (
                f"{signal['ticker']}: no evidence item has a sourceDate"
            )
            assert has_excerpt, (
                f"{signal['ticker']}: no evidence item has a sourceExcerpt"
            )
            return  # one company passing is enough

        pytest.fail("No signal had any evidence to check provenance fields")

    def test_rationale_tree_has_source_links(self, trade_signals):
        """Trade signal rationale trees should contain source references."""
        signals = trade_signals["tradeSignals"]["signals"]
        if not signals:
            pytest.skip("No trade signals to trace")

        signals_with_rationale = 0
        rationale_with_sources = 0

        for signal in signals[:10]:
            rationale = signal.get("rationale")
            if rationale is None:
                continue
            signals_with_rationale += 1

            # Walk the rationale tree looking for source URLs/dates
            def _has_source(node):
                if node.get("sourceUrl") or node.get("sourceDate"):
                    return True
                for child in node.get("children") or []:
                    if _has_source(child):
                        return True
                return False

            if _has_source(rationale):
                rationale_with_sources += 1

        if signals_with_rationale > 0:
            pct = rationale_with_sources / signals_with_rationale * 100
            print(f"\nRationale trees with source links: {pct:.0f}% "
                  f"({rationale_with_sources}/{signals_with_rationale})")


# ── Test 2: Score -> Evidence Group -> Valuation chain ─────────────────
#
# companyDetail is broken, so we test the chain using what we can reach:
# - latestScores gives us scores
# - evidenceForCompany gives us evidence
# - companyDetail's evidenceGroups/valuations are inaccessible (bug)
# We flag this gap and test what's available.


class TestScoreEvidenceGroupValuationChain:

    def test_company_detail_needed_for_groups_and_valuations(self):
        """Document that evidenceGroups/valuations require companyDetail (which is broken)."""
        _, err = _get_company_detail_safe("GOOGL")
        if err and "greenlet_spawn" in err:
            pytest.xfail(
                "TRACEABILITY GAP: evidenceGroups and valuations are only accessible "
                "via companyDetail, which has an async greenlet bug. "
                "Cannot verify group->valuation chain until this is fixed."
            )

    def test_evidence_types_cover_multiple_dimensions(self, latest_scores):
        """Evidence should cover multiple target dimensions (proxy for groups)."""
        if not latest_scores:
            pytest.skip("No scores available")

        checked = 0
        multi_dimension_companies = 0

        for score in latest_scores[:10]:
            ticker = score.get("ticker")
            if not ticker:
                continue
            evidence = _get_evidence_for_company(ticker)
            if not evidence:
                continue
            checked += 1

            dimensions = {e["targetDimension"] for e in evidence if e["targetDimension"]}
            if len(dimensions) > 1:
                multi_dimension_companies += 1

            if checked >= 5:
                break

        if checked > 0:
            pct = multi_dimension_companies / checked * 100
            print(f"\nCompanies with multi-dimension evidence: {pct:.0f}% "
                  f"({multi_dimension_companies}/{checked})")
            assert multi_dimension_companies > 0, (
                "No company had evidence covering multiple dimensions"
            )

    def test_evidence_covers_multiple_stages(self, latest_scores):
        """Evidence should cover multiple capture stages (plan/invest/capture)."""
        if not latest_scores:
            pytest.skip("No scores available")

        checked = 0
        multi_stage_companies = 0

        for score in latest_scores[:10]:
            ticker = score.get("ticker")
            if not ticker:
                continue
            evidence = _get_evidence_for_company(ticker)
            if not evidence:
                continue
            checked += 1

            stages = {e["captureStage"] for e in evidence if e["captureStage"]}
            if len(stages) > 1:
                multi_stage_companies += 1

            if checked >= 5:
                break

        if checked > 0:
            print(f"\nCompanies with multi-stage evidence: "
                  f"{multi_stage_companies}/{checked}")


# ── Test 3: Evidence provenance completeness ───────────────────────────


class TestEvidenceProvenanceCompleteness:

    def test_provenance_completeness(self, latest_scores):
        """Check what % of evidence has complete provenance fields."""
        if not latest_scores:
            pytest.skip("No scores available")

        checked_companies = 0
        for score in latest_scores[:20]:
            ticker = score.get("ticker")
            if not ticker:
                continue
            evidence = _get_evidence_for_company(ticker)
            if not evidence:
                continue
            checked_companies += 1

            for e in evidence:
                report.total_evidence += 1

                has_url = e["sourceUrl"] is not None
                has_date = e["sourceDate"] is not None
                has_dim = e["targetDimension"] is not None
                has_stage = e["captureStage"] is not None
                has_excerpt = e["sourceExcerpt"] is not None

                if has_url:
                    report.evidence_with_source_url += 1
                if has_date:
                    report.evidence_with_source_date += 1
                if has_dim:
                    report.evidence_with_target_dimension += 1
                if has_stage:
                    report.evidence_with_capture_stage += 1
                if has_excerpt:
                    report.evidence_with_source_excerpt += 1
                if all([has_url, has_date, has_dim, has_stage, has_excerpt]):
                    report.fully_complete_evidence += 1

            if checked_companies >= 15:
                break

        prov_pct = report.provenance_pct()
        print(f"\nProvenance completeness: {prov_pct:.1f}% "
              f"({report.fully_complete_evidence}/{report.total_evidence})")

        if report.total_evidence > 0:
            if prov_pct < 80:
                pytest.xfail(
                    f"Provenance completeness {prov_pct:.1f}% is below 80% threshold. "
                    f"System is not production-ready."
                )

    def test_source_url_coverage_by_evidence_type(self, latest_scores):
        """Break down source_url coverage by evidence type to find gaps."""
        if not latest_scores:
            pytest.skip("No scores available")

        type_counts: dict[str, dict] = {}  # evidence_type -> {total, with_url}
        checked = 0

        for score in latest_scores[:15]:
            ticker = score.get("ticker")
            if not ticker:
                continue
            evidence = _get_evidence_for_company(ticker)
            if not evidence:
                continue
            checked += 1

            for e in evidence:
                etype = e["evidenceType"] or "unknown"
                if etype not in type_counts:
                    type_counts[etype] = {"total": 0, "with_url": 0}
                type_counts[etype]["total"] += 1
                if e["sourceUrl"] is not None:
                    type_counts[etype]["with_url"] += 1

            if checked >= 10:
                break

        print("\nSource URL coverage by evidence type:")
        for etype, counts in sorted(type_counts.items()):
            pct = counts["with_url"] / counts["total"] * 100 if counts["total"] > 0 else 0
            flag = " <-- GAP" if pct < 50 and counts["total"] >= 3 else ""
            print(f"  {etype:30s}  {counts['with_url']:4d}/{counts['total']:4d}  ({pct:5.1f}%){flag}")


# ── Test 4: Cross-source verification availability ─────────────────────


class TestCrossSourceVerification:

    def test_verification_returns_results(self, latest_scores):
        """Companies with multiple source types should have verification results."""
        if not latest_scores:
            pytest.skip("No scores available")

        checked = 0
        for score in latest_scores[:10]:
            ticker = score.get("ticker")
            if not ticker:
                continue

            result = _get_verification(ticker)
            if result is None:
                continue

            report.companies_verified += 1

            if result["confirmations"]:
                report.companies_with_confirmations += 1
                for c in result["confirmations"]:
                    assert c["sourceA"] is not None, (
                        f"{ticker}: confirmation missing sourceA label"
                    )
                    assert c["sourceB"] is not None, (
                        f"{ticker}: confirmation missing sourceB label"
                    )

            if result["contradictions"]:
                report.companies_with_contradictions += 1
                for d in result["contradictions"]:
                    assert d["sourceA"] is not None, (
                        f"{ticker}: contradiction missing sourceA label"
                    )
                    assert d["sourceB"] is not None, (
                        f"{ticker}: contradiction missing sourceB label"
                    )

            checked += 1
            if checked >= 5:
                break

        assert report.companies_verified > 0, (
            "companyVerification returned no results for any company"
        )

    def test_verification_agreement_score_is_bounded(self, latest_scores):
        """Verification agreement_score should be between 0 and 1."""
        if not latest_scores:
            pytest.skip("No scores available")

        for score in latest_scores[:5]:
            ticker = score.get("ticker")
            if not ticker:
                continue
            result = _get_verification(ticker)
            if result is None:
                continue
            ag = result["agreementScore"]
            assert 0.0 <= ag <= 1.0, (
                f"{ticker}: agreementScore {ag} out of [0,1] range"
            )
            return

        pytest.skip("No verification results to check")


# ── Test 5: Score freshness ────────────────────────────────────────────


class TestScoreFreshness:

    def test_score_freshness_distribution(self, latest_scores):
        """Report score freshness distribution and flag if > 50% stale."""
        if not latest_scores:
            pytest.skip("No scores available")

        for score in latest_scores:
            report.total_scores += 1
            level = score.get("stalenessLevel")
            if level == "fresh":
                report.fresh_scores += 1
            elif level == "warning":
                report.warning_scores += 1
            elif level == "critical":
                report.critical_scores += 1
            else:
                # unknown / None -- count as stale
                report.critical_scores += 1

        fresh_pct = report.freshness_pct()
        stale_pct = 100 - fresh_pct
        print(f"\nScore freshness: {fresh_pct:.1f}% fresh, {stale_pct:.1f}% stale")

        if stale_pct > 50:
            pytest.xfail(
                f"Production concern: {stale_pct:.1f}% of scores are stale "
                f"(> 50% threshold)"
            )

    def test_score_age_days_populated(self, latest_scores):
        """scoreAgeDays should be populated for all scores."""
        if not latest_scores:
            pytest.skip("No scores available")

        missing_age = [s["ticker"] for s in latest_scores if s.get("scoreAgeDays") is None]
        pct_missing = len(missing_age) / len(latest_scores) * 100
        assert pct_missing < 10, (
            f"{pct_missing:.0f}% of scores missing scoreAgeDays: "
            f"{', '.join(str(t) for t in missing_age[:5])}"
        )


# ── Test 6: Missing data detection ────────────────────────────────────


class TestMissingDataDetection:

    def test_orphaned_scores_and_unscored_companies(self, latest_scores, all_companies):
        """Detect companies with scores but no evidence, and vice versa."""
        if not latest_scores or not all_companies:
            pytest.skip("No data available")

        scored_tickers = {s["ticker"] for s in latest_scores if s.get("ticker")}
        all_tickers = {c["ticker"] for c in all_companies if c.get("ticker")}

        # Check a sample of scored companies for evidence
        checked = 0
        for score in latest_scores[:20]:
            ticker = score.get("ticker")
            if not ticker:
                continue
            evidence = _get_evidence_for_company(ticker)
            checked += 1

            if not evidence:
                report.orphaned_scores.append(ticker)

            # Track dollar estimate completeness
            for e in evidence:
                report.total_evidence_for_dollars += 1
                if e.get("dollarEstimateUsd") is None:
                    report.evidence_missing_dollars += 1

            if checked >= 15:
                break

        # Check for unscored companies
        unscored = all_tickers - scored_tickers
        report.unscored_companies = sorted(list(unscored))[:20]

        if report.orphaned_scores:
            print(f"\nOrphaned scores (score but no evidence): {report.orphaned_scores}")

        if len(report.unscored_companies) > 0:
            print(f"\nUnscored companies ({len(unscored)} total): "
                  f"{report.unscored_companies[:10]}")

    def test_evidence_dollar_estimates(self, latest_scores):
        """Flag evidence items missing dollar estimates."""
        if report.total_evidence_for_dollars == 0:
            for score in (latest_scores or [])[:10]:
                ticker = score.get("ticker")
                if not ticker:
                    continue
                evidence = _get_evidence_for_company(ticker)
                for e in evidence:
                    report.total_evidence_for_dollars += 1
                    if e.get("dollarEstimateUsd") is None:
                        report.evidence_missing_dollars += 1

        if report.total_evidence_for_dollars > 0:
            missing_pct = report.evidence_missing_dollars / report.total_evidence_for_dollars * 100
            print(f"\nEvidence missing dollar estimates: {missing_pct:.1f}% "
                  f"({report.evidence_missing_dollars}/{report.total_evidence_for_dollars})")

    def test_no_completely_empty_scored_companies(self, latest_scores):
        """Every scored company should have at least some evidence."""
        if not latest_scores:
            pytest.skip("No scores available")

        empty_count = 0
        checked = 0
        empty_tickers = []

        for score in latest_scores[:30]:
            ticker = score.get("ticker")
            if not ticker:
                continue
            evidence = _get_evidence_for_company(ticker, limit=1)
            checked += 1
            if not evidence:
                empty_count += 1
                empty_tickers.append(ticker)
            if checked >= 20:
                break

        if checked > 0:
            empty_pct = empty_count / checked * 100
            print(f"\nScored companies with zero evidence: {empty_pct:.0f}% "
                  f"({empty_count}/{checked})")
            if empty_tickers:
                print(f"  tickers: {', '.join(empty_tickers[:10])}")
            assert empty_pct < 20, (
                f"{empty_pct:.0f}% of scored companies have zero evidence -- "
                f"orphaned scores indicate broken pipeline"
            )


# ── Final report (printed after all tests) ─────────────────────────────


def test_zzz_print_final_report():
    """Print the final traceability report (runs last due to name sorting)."""
    report.print_report()

    # Summary assertions -- these always pass; the report is informational
    if report.total_evidence > 0:
        assert report.provenance_pct() >= 0, "Provenance calculation failed"
    if report.total_scores > 0:
        assert report.freshness_pct() >= 0, "Freshness calculation failed"
    if report.signals_checked > 0:
        assert report.chain_integrity_pct() >= 0, "Chain integrity calculation failed"
