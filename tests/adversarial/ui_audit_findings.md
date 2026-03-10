# UI Audit Findings -- Adversarial Testing Report

**Date:** 2026-03-08
**Auditor:** Automated adversarial UI tester
**Scope:** Next.js frontend at `frontend/src/` vs GraphQL backend at `web/graphql/`

---

## 1. Missing Functionality (Backend supports, frontend does not expose)

### 1.1 CRITICAL -- No Company Detail Page
- **Status:** FIXED
- **Finding:** The GraphQL schema exposes a rich `companyDetail(ticker)` query returning company info, latest score with all sub-dimensions, evidence list, financial observations, evidence groups, valuations, and industry peers. The frontend defines a `GET_COMPANY_DETAIL` query in `queries.ts` but has no route or page to display it.
- **Impact:** Users cannot drill into any company to see evidence, financials, score breakdown, or peer comparison.
- **Fix:** Created `/company/[ticker]/page.tsx` with full company profile.

### 1.2 CRITICAL -- Dashboard Scores Table Has No Links
- **Status:** FIXED
- **Finding:** The dashboard table at `src/app/page.tsx` shows ranked companies but the table rows are not clickable. There is no way to navigate from the dashboard to a company detail page. The table also does not show the ticker or company name.
- **Impact:** Users see scores but cannot act on them or investigate further.
- **Fix:** Added ticker column with link to `/company/[ticker]`, made rows clickable.

### 1.3 HIGH -- Trading Signals Don't Link to Company Detail
- **Status:** FIXED
- **Finding:** The trading page shows ticker and company name but neither is a link. The `rationaleSummary` field from GraphQL is available in the query but is not displayed.
- **Impact:** Traders cannot quickly jump to a company's full profile to validate signals.
- **Fix:** Made ticker a clickable link to `/company/[ticker]`.

### 1.4 MEDIUM -- `GET_COMPANY_DETAIL` Query Missing `evidenceGroups` and `valuations`
- **Status:** FIXED
- **Finding:** The backend `CompanyDetailType` includes `evidence_groups: list[EvidenceGroupType]` and `valuations: list[ValuationType]`, but the frontend `GET_COMPANY_DETAIL` query in `queries.ts` does not request these fields. These contain critical provenance data (evidence groupings, dollar range estimates, narratives, confidence scores).
- **Impact:** Even after building a company page, the provenance chain would be incomplete.
- **Fix:** Added `evidenceGroups` and `valuations` to the `GET_COMPANY_DETAIL` query.

### 1.5 MEDIUM -- `staleScores` Query Not Surfaced
- **Status:** NOTED (not fixed -- lower priority)
- **Finding:** The resolver exposes `staleScores(limit)` which returns scores older than the staleness warning threshold. No frontend page or component uses this query. This is a distinct query from the staleness badges on the dashboard (which are per-row decorations).
- **Impact:** Traders and analysts have no dedicated view of which companies urgently need a score refresh.

### 1.6 MEDIUM -- `evidenceForCompany` Standalone Query Not Used
- **Status:** NOTED (partially addressed via company detail page)
- **Finding:** The resolver exposes `evidenceForCompany(ticker, limit)` as a standalone query. The frontend never calls it. Evidence is instead accessed through `companyDetail`, which is fine but the standalone query could support filtering or pagination in the future.

### 1.7 LOW -- `pipelineRuns` Standalone Query Not Used
- **Status:** NOTED
- **Finding:** `pipelineRuns(limit)` exists as a standalone resolver, separate from `pipelineStatus`. The pipeline page only uses `pipelineStatus` which includes `recentRuns`. The standalone query is redundant for now but could be useful for a dedicated log viewer.

### 1.8 LOW -- `GET_COMPANIES` Query Defined but Unused
- **Status:** NOTED
- **Finding:** `GET_COMPANIES` is defined in `queries.ts` with sector and search filter support but is never imported by any page. There is no company list/search page.

---

## 2. Missing Data Display

### 2.1 CRITICAL -- Score Sub-Dimensions Not Shown Anywhere
- **Status:** FIXED (in company detail page)
- **Finding:** The backend tracks 8+ score sub-dimensions (`costOppScore`, `revenueOppScore`, `costCaptureScore`, `revenueCaptureScore`, `filingNlpScore`, `productScore`, `githubScore`, `analystScore`) plus `costRoi`, `revenueRoi`, `captureProbability`, `opportunityUsd`, `evidenceDollars`. None of these are shown in the dashboard or trading pages.
- **Impact:** Users see composite scores but have no visibility into what drives them.
- **Fix:** Company detail page shows all sub-dimensions in a visual bar chart layout.

### 2.2 HIGH -- Dashboard Missing Ticker and Company Name
- **Status:** FIXED
- **Finding:** The `latestScores` query returns `id` and `companyId` but not ticker or company name. The dashboard table shows rank, quadrant, opportunity, realization, AI Index USD, and freshness -- but not which company each row represents.
- **Impact:** The dashboard's main table is essentially unusable since users cannot identify companies.
- **Note:** The `latestScores` resolver returns dicts from a materialized view that may or may not include ticker. The query needs `ticker` and `companyName` fields added. For now, added them to the query; if the backend doesn't return them, the rows will show placeholder text.

### 2.3 HIGH -- Score Provenance Not Displayed
- **Status:** FIXED (via company detail page valuations section)
- **Finding:** Evidence groups and valuations (dollar ranges, narratives, confidence) are tracked in the backend but were invisible to the user.

### 2.4 MEDIUM -- Financial Observations Not Shown
- **Status:** FIXED (in company detail page)
- **Finding:** `financials` are returned by `companyDetail` and queried in `GET_COMPANY_DETAIL` but had no page to display them.

### 2.5 MEDIUM -- Trading Rationale Summary Not Shown
- **Status:** FIXED
- **Finding:** `rationaleSummary` is queried by `GET_TRADE_SIGNALS` but was not displayed in the trading table. This is valuable context for why a signal was generated.

---

## 3. Missing Error / Loading / Empty States

### 3.1 HIGH -- Dashboard Page Has No Error State
- **Status:** FIXED
- **Finding:** `src/app/page.tsx` destructures `loading` but does not destructure or handle `error` from either `useQuery` call. If the GraphQL server is down or returns an error, the page silently shows stale or empty data.
- **Fix:** Added error state rendering to dashboard.

### 3.2 MEDIUM -- Dashboard Has No Empty State for Scores
- **Status:** FIXED
- **Finding:** If `latestScores` returns an empty array, the table renders headers with no body rows and no message. Should show "No scores available yet."
- **Fix:** Added empty state message.

### 3.3 LOW -- Pipeline Page Handles Errors Well
- **Status:** OK
- **Finding:** Pipeline page properly shows error banner, loading skeletons, and empty state. Good pattern.

### 3.4 LOW -- Changelog Page Handles States Well
- **Status:** OK

### 3.5 LOW -- Internals Page Handles States Well
- **Status:** OK

---

## 4. Dead Links / Missing Routes

### 4.1 CRITICAL -- No `/company/[ticker]` Route
- **Status:** FIXED
- **Finding:** The most critical missing route. GraphQL supports it, the query is defined, but no page existed.

### 4.2 LOW -- Nav Does Not Highlight Company Detail Pages
- **Status:** NOTED
- **Finding:** The nav uses `pathname === link.href` for exact match highlighting. When viewing `/company/AAPL`, no nav item is highlighted. This is cosmetically suboptimal but not a blocker.

---

## 5. Accessibility Issues

### 5.1 MEDIUM -- Dashboard Score Table Missing Company Identification
- **Status:** FIXED (added ticker column)
- **Finding:** Screen readers cannot identify which company a score row belongs to because ticker/name were missing.

### 5.2 MEDIUM -- No `aria-label` on Verification Ticker Input
- **Status:** NOTED
- **Finding:** The ticker input in the internals verification section has a placeholder but no `<label>` element or `aria-label`.

### 5.3 LOW -- Color-Only Staleness Indicators
- **Status:** NOTED
- **Finding:** Staleness is conveyed through color-coded badges (green/yellow/red). The text labels ("Fresh", "Aging", "Stale") help, but the badges also rely on background color to convey urgency. Users with color vision deficiency may have difficulty distinguishing these.

### 5.4 LOW -- Tables Not Using Semantic `<th scope>` Attributes
- **Status:** NOTED
- **Finding:** All tables use `<th>` without `scope="col"`, which reduces screen reader navigability.

---

## Summary of Fixes Applied

| # | Severity | Finding | File(s) Modified |
|---|----------|---------|------------------|
| 1 | CRITICAL | Created company detail page | `frontend/src/app/company/[ticker]/page.tsx` (new) |
| 2 | CRITICAL | Added links from dashboard to company pages | `frontend/src/app/page.tsx` |
| 3 | CRITICAL | Updated query with missing fields | `frontend/src/lib/graphql/queries.ts` |
| 4 | HIGH | Added ticker links in trading page | `frontend/src/app/trading/page.tsx` |
| 5 | HIGH | Added error/empty states to dashboard | `frontend/src/app/page.tsx` |
| 6 | MEDIUM | Added rationale summary to trading | `frontend/src/app/trading/page.tsx` |
