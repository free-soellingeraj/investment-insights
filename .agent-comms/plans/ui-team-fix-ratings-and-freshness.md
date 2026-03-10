# Plan: Fix Rating Submission and Add Freshness Indicators

## Context

Analysis of user feedback and production data reveals two bugs and two UX gaps:

1. **All 4 human ratings have NULL numeric values** -- the rating widget is broken.
2. **Passage count mismatch** -- evidence group cards show stale `passage_count` that diverges from actual passage rows.
3. **No freshness indicator on main dashboard** -- staleness data exists in GraphQL but the main company list (`/`) does not display it.
4. **Low rating adoption** (4 ratings total) -- the rating widget is hard to discover (40% opacity, tiny icons).

---

## Root Cause Analysis

### Bug 1: Rating Values Are Always NULL

**Root cause**: Field name mismatch between frontend and backend.

- Frontend (`rating-widget.tsx:191`) sends `thumb: 'up' | 'down' | null` in the POST body.
- Backend (`ratings_api.py:62`) reads `body.get("rating")` which is never set by the frontend.
- The `thumb` field is ignored by the backend entirely -- it is not mapped to `rating`.
- Result: `create_human_rating()` always receives `rating=None`.

**File**: `frontend/src/components/rating-widget.tsx:188-197` (request body construction)
**File**: `web/ratings_api.py:58-67` (parameter extraction)

### Bug 2: Passage Count Mismatch

**Root cause**: `passage_count` on `evidence_groups` is a denormalized integer set at group creation time (`evidence_munger.py:417`) but never updated when passages are added/removed in subsequent pipeline runs.

- The `EvidenceGroupModel.passage_count` column stores the count at creation.
- The frontend reads `passageCount` from GraphQL and displays it on the top-level card.
- When expanded, the frontend shows actual loaded passages (which may differ).
- The `_group_to_gql` converter (`resolvers.py:940-955`) trusts the stored `passage_count`.

**File**: `ai_opportunity_index/scoring/evidence_munger.py:417` (count set at creation)
**File**: `ai_opportunity_index/storage/repositories/valuation.py:319` (count read from model)
**File**: `web/graphql/resolvers.py:940-955` (`_group_to_gql` converter)

### UX Gap 1: No Staleness on Main Dashboard

The GraphQL `latestScores` query already returns `scoreAgeDays` and `stalenessLevel` (computed in `resolvers.py:51-66`), and the company detail page uses them (`company/[ticker]/page.tsx:541-546`). However, the main dashboard page (`/`) does not render these fields despite requesting them in the query.

### UX Gap 2: Rating Widget Hard to Find

The rating widget renders at 40% opacity (`opacity-40`) with 14px thumb icons. Users must hover to see it, and the expand button (3 dots) is only 10px. Combined with no onboarding prompt, this explains the low adoption rate (4 ratings across the entire platform).

---

## Proposed Actions

### Action 1: Fix Rating Field Mapping (Priority: Critical)

**Option A (recommended)**: Map `thumb` to `rating` in the backend.

In `web/ratings_api.py`, after extracting `body`, add mapping logic:

```python
# Map frontend 'thumb' field to numeric rating
thumb = body.get("thumb")
if thumb == "up":
    rating = 1
elif thumb == "down":
    rating = -1
else:
    rating = body.get("rating")  # fallback for direct API callers
```

Pass `rating` to `create_human_rating()` instead of `body.get("rating")`.

Also store `stars` dimensions: the frontend sends `body.stars` (an object with accuracy/relevance/dollar_estimate/overall) but the backend ignores it. Store it in the `metadata_extra` JSON column by merging into the metadata dict.

**Files to change**:
- `web/ratings_api.py:58-67` -- add thumb-to-rating mapping and stars extraction

### Action 2: Fix Passage Count (Priority: High)

Replace the stored `passage_count` with a live count from the actual passage rows in the GraphQL resolver.

In `valuation.py:get_evidence_groups_for_company()`, after loading passages for each group (line 309-312), set `passage_count` to `len(passage_models)` instead of using `m.passage_count`.

**Preferred approach**: Change line 319 in `valuation.py` from:
```python
passage_count=m.passage_count,
```
to:
```python
passage_count=len(passage_models),
```

This is safe because we already load `passage_models` in the same method (line 303-311), so there is no extra DB round-trip.

**Files to change**:
- `ai_opportunity_index/storage/repositories/valuation.py:319` -- use `len(passage_models)` for passage_count

### Action 3: Add Staleness Badge to Main Dashboard (Priority: Medium)

The main dashboard page at `frontend/src/app/page.tsx` already queries `scoreAgeDays` and `stalenessLevel` via GraphQL but does not render them. Add a small colored badge next to each company's score row:

- `fresh` (< warning threshold): no badge
- `warning`: amber badge showing days (e.g., "7d")
- `critical`: red badge showing days (e.g., "14d")

**Files to change**:
- `frontend/src/app/page.tsx` -- add staleness badge to score row rendering

### Action 4: Improve Rating Widget Discoverability (Priority: Medium)

1. Increase base opacity from `opacity-40` to `opacity-60`.
2. Increase thumb icon size from 14px to 18px.
3. Add a subtle tooltip on first visit ("Rate this data point") using localStorage to track dismissal.
4. On the company detail page, add a prominent "Rate this company" CTA near the score section.

**Files to change**:
- `frontend/src/components/rating-widget.tsx:255` -- increase opacity
- `frontend/src/components/rating-widget.tsx:45-46` -- increase icon size
- `frontend/src/app/company/[ticker]/page.tsx` -- add rating CTA near score

---

## Files Likely Affected

| File | Change Type |
|------|-------------|
| `web/ratings_api.py` | Bug fix: thumb-to-rating mapping |
| `ai_opportunity_index/storage/repositories/valuation.py` | Bug fix: live passage count |
| `frontend/src/app/page.tsx` | Feature: staleness badges |
| `frontend/src/components/rating-widget.tsx` | UX: discoverability improvements |
| `frontend/src/app/company/[ticker]/page.tsx` | UX: rating CTA |

---

## Success Criteria

1. **Rating fix verified**: New ratings submitted via the widget have non-NULL `rating` values in the `human_ratings` table. Verify with: `SELECT id, rating, action FROM human_ratings ORDER BY created_at DESC LIMIT 10;`
2. **Passage count accurate**: For any evidence group, the `passageCount` in GraphQL matches `SELECT COUNT(*) FROM evidence_group_passages WHERE group_id = <id>`.
3. **Staleness visible**: On the main dashboard, companies with scores older than `SCORE_STALENESS_WARNING_DAYS` show a colored age badge.
4. **Rating adoption increase**: After deployment, monitor for > 4 new ratings within the first week (vs. 4 total over prior period).

---

## Estimated Effort

- Action 1 (rating fix): ~30 min -- single file change, straightforward field mapping
- Action 2 (passage count): ~30 min -- repository method adjustment
- Action 3 (staleness badges): ~45 min -- frontend component addition
- Action 4 (widget discoverability): ~45 min -- CSS/layout tweaks + localStorage logic

## Risk Assessment

- **Low risk**: All changes are additive or corrective. No schema migrations needed.
- **Rating fix**: Backward compatible -- direct API callers sending `rating` field still work via fallback.
- **Passage count**: Performance consideration -- we already load passages in the query, so `len()` adds no extra DB round-trip.
