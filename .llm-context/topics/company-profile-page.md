# Company Profile Page

**File**: `web/landing/company_profile.html`

## Section Order (top to bottom)

| # | Section | Description |
|---|---------|-------------|
| 1 | **Header** | Company name, ticker/slug, sector/industry/exchange, quadrant badge, flags |
| 2 | **Listing Information** | Alias/share-class banners (secondary listing, primary listing), parent company banner (subsidiary status with ownership %) |
| 3 | **High-Level Financials** | Grid of Market Cap, Revenue, Net Income, Employees cards |
| 4 | **AI Opportunity ($)** | Hero card: `AI Opportunity = P(capture) x Opportunity` with cost savings + revenue opportunity breakdown, evidence basis line |
| 5 | **Evidence Quality Summary** | Pipeline stats (passages → groups → dimensions), stage breakdown (plan/invest/capture), avg specificity/recency, per-dimension dollar estimates. Populated async after valuations load. |
| 6 | **Company Information** | Editable fields (name, ticker, slug, exchange, sector, industry, URLs). Subsidiaries/ventures list with pipeline progress. Parent company links. |
| 7 | **Pipeline Status** | 9-stage pipeline matrix (discover_links through score). Progress bar, "Run Whole Pipeline" button. Each stage is clickable to run individually. |
| 8 | **Pipeline Runs** | Collapsible list of recent pipeline runs with status, duration, error messages. Polls while runs are active. |
| 9 | **Evidence** | Scrollable container (`max-h-[800px]`) containing: |
|   | — Valuation Viewer | Filter bar (source/target/stage/sort) + evidence group cards with expandable details (quality indicators, dollar ranges, source passages) |
|   | — GitHub Signals | Collapsible enrichment: repo counts, AI/ML repos, top repos, star counts |
|   | — Analyst Consensus | Collapsible enrichment: recommendation, price targets, analyst count |
|   | — Web Intelligence | Collapsible enrichment: careers/IR/blog page analysis with classified evidence items |
|   | — Legacy Evidence | Fallback when no valuation data exists |
| 10 | **Industry Peers** | Peer comparison table: ticker, company, cost/revenue capture, ROI, quadrant |

## Key Design Decisions

- **Deprecated sections** (removed): ROI Summary, Opportunity Scores, Capture Scores, Dollar Estimates (3-Year Horizon). These were replaced by the AI Opportunity hero card.
- **Evidence scroller**: All evidence content (valuations, GitHub, analyst, web intel) lives inside a scrollable `max-h-[800px]` container to prevent the page from extending infinitely.
- **Flags**: Rendered as small badges in the header next to the quadrant badge, not as a separate section.
- **Slug-based companies**: All sections handle tickerless subsidiaries via `company.ticker or company.slug` fallback pattern.

## Async-Loaded Sections

These sections render placeholder divs in the initial HTML and populate via JS after the main `load()` completes:

| Section | Function | Endpoint |
|---------|----------|----------|
| Evidence Quality Summary | `renderEvidenceQuality()` | (derived from valuations) |
| Company Information | `loadCompanyLinks()` | `GET /api/companies/{ticker}/links` |
| Pipeline Status | `loadPipelineStatus()` | `GET /api/companies/{ticker}/pipeline-status` |
| Pipeline Runs | `loadCompanyRuns()` | `GET /api/companies/{ticker}/runs` |
| Valuation Viewer | `loadValuations()` | `GET /api/companies/{ticker}/valuations` |
