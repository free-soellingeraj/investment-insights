"""Audit: opportunity_usd vs evidence_dollars divergence across all scored companies."""

import sqlalchemy as sa
from sqlalchemy import create_engine, text

DB_URL = "postgresql://free-soellingeraj@localhost:5432/ai_opportunity_index"

engine = create_engine(DB_URL)

query = text("""
    SELECT
        c.ticker,
        c.company_name,
        cs.id AS score_id,
        cs.ai_index_usd,
        cs.opportunity_usd,
        cs.evidence_dollars,
        cs.capture_probability,
        cs.opportunity,
        cs.realization,
        cs.quadrant,
        cs.scored_at,
        cs.flags
    FROM company_scores cs
    JOIN companies c ON c.id = cs.company_id
    WHERE cs.id IN (
        SELECT DISTINCT ON (company_id) id
        FROM company_scores
        ORDER BY company_id, scored_at DESC
    )
    ORDER BY cs.ai_index_usd DESC NULLS LAST
""")

with engine.connect() as conn:
    rows = conn.execute(query).fetchall()

print(f"Total scored companies: {len(rows)}\n")

# Categorize issues
issues_ratio_100x = []  # opp_usd / evidence_dollars > 100
issues_5x_cap = []      # ai_index_usd > 5x evidence_dollars
issues_zero_evidence = []  # evidence_dollars is 0/null but ai_index_usd > 0
all_ratios = []

for r in rows:
    ticker = r.ticker or "(no ticker)"
    name = r.company_name or "(unnamed)"
    ai_idx = r.ai_index_usd or 0
    opp_usd = r.opportunity_usd or 0
    ev_dollars = r.evidence_dollars or 0
    cap_prob = r.capture_probability or 0

    # Compute ratios
    if ev_dollars > 0:
        opp_ratio = opp_usd / ev_dollars
        ai_ratio = ai_idx / ev_dollars
    else:
        opp_ratio = float('inf') if opp_usd > 0 else 0
        ai_ratio = float('inf') if ai_idx > 0 else 0

    entry = {
        "ticker": ticker,
        "name": name,
        "ai_index_usd": ai_idx,
        "opportunity_usd": opp_usd,
        "evidence_dollars": ev_dollars,
        "capture_probability": cap_prob,
        "opp_ratio": opp_ratio,
        "ai_ratio": ai_ratio,
        "quadrant": r.quadrant,
        "flags": r.flags,
    }
    all_ratios.append(entry)

    if ev_dollars == 0 and ai_idx > 0:
        issues_zero_evidence.append(entry)
    if opp_ratio > 100:
        issues_ratio_100x.append(entry)
    if ai_ratio > 5:
        issues_5x_cap.append(entry)


def fmt_usd(v):
    if v == 0:
        return "$0"
    if abs(v) >= 1e9:
        return f"${v/1e9:.2f}B"
    if abs(v) >= 1e6:
        return f"${v/1e6:.2f}M"
    if abs(v) >= 1e3:
        return f"${v/1e3:.1f}K"
    return f"${v:.0f}"


def fmt_ratio(v):
    if v == float('inf'):
        return "INF"
    return f"{v:.1f}x"


print("=" * 100)
print("ISSUE 1: opportunity_usd / evidence_dollars > 100x")
print("=" * 100)
issues_ratio_100x.sort(key=lambda x: x["opp_ratio"], reverse=True)
if issues_ratio_100x:
    for i, e in enumerate(issues_ratio_100x[:20]):
        print(f"  {i+1:3d}. {e['ticker']:8s} | opp={fmt_usd(e['opportunity_usd']):>12s} | ev={fmt_usd(e['evidence_dollars']):>12s} | ratio={fmt_ratio(e['opp_ratio']):>8s} | {e['name'][:40]}")
    print(f"  ... {len(issues_ratio_100x)} total companies with >100x ratio")
else:
    print("  NONE - All companies have opp/evidence ratio <= 100x")

print()
print("=" * 100)
print("ISSUE 2: ai_index_usd > 5x evidence_dollars (should be capped)")
print("=" * 100)
issues_5x_cap.sort(key=lambda x: x["ai_ratio"], reverse=True)
if issues_5x_cap:
    for i, e in enumerate(issues_5x_cap[:20]):
        print(f"  {i+1:3d}. {e['ticker']:8s} | ai_idx={fmt_usd(e['ai_index_usd']):>12s} | ev={fmt_usd(e['evidence_dollars']):>12s} | ratio={fmt_ratio(e['ai_ratio']):>8s} | {e['name'][:40]}")
    print(f"  ... {len(issues_5x_cap)} total companies with ai_index > 5x evidence")
else:
    print("  NONE - All companies have ai_index_usd <= 5x evidence_dollars")

print()
print("=" * 100)
print("ISSUE 3: evidence_dollars = 0 but ai_index_usd > 0")
print("=" * 100)
issues_zero_evidence.sort(key=lambda x: x["ai_index_usd"], reverse=True)
if issues_zero_evidence:
    for i, e in enumerate(issues_zero_evidence[:20]):
        print(f"  {i+1:3d}. {e['ticker']:8s} | ai_idx={fmt_usd(e['ai_index_usd']):>12s} | ev={fmt_usd(e['evidence_dollars']):>12s} | cap_prob={e['capture_probability']:.2f} | {e['name'][:40]}")
    print(f"  ... {len(issues_zero_evidence)} total companies with zero evidence but nonzero ai_index")
else:
    print("  NONE - All companies with ai_index_usd > 0 have evidence_dollars > 0")

print()
print("=" * 100)
print("SUMMARY: Top 20 companies by ai_index_usd")
print("=" * 100)
all_ratios.sort(key=lambda x: x["ai_index_usd"], reverse=True)
for i, e in enumerate(all_ratios[:20]):
    print(f"  {i+1:3d}. {e['ticker']:8s} | ai_idx={fmt_usd(e['ai_index_usd']):>12s} | opp={fmt_usd(e['opportunity_usd']):>12s} | ev={fmt_usd(e['evidence_dollars']):>12s} | ai/ev={fmt_ratio(e['ai_ratio']):>8s} | q={str(e['quadrant']):>20s} | {e['name'][:30]}")

print()
print("=" * 100)
print("DISTRIBUTION: ai_index / evidence_dollars ratio")
print("=" * 100)
# Only for companies with evidence_dollars > 0
with_ev = [e for e in all_ratios if e["evidence_dollars"] > 0]
if with_ev:
    ratios = [e["ai_ratio"] for e in with_ev]
    ratios.sort()
    print(f"  Companies with evidence_dollars > 0: {len(with_ev)}")
    print(f"  Min ratio:    {fmt_ratio(ratios[0])}")
    print(f"  Median ratio: {fmt_ratio(ratios[len(ratios)//2])}")
    print(f"  P90 ratio:    {fmt_ratio(ratios[int(len(ratios)*0.9)])}")
    print(f"  P99 ratio:    {fmt_ratio(ratios[int(len(ratios)*0.99)])}")
    print(f"  Max ratio:    {fmt_ratio(ratios[-1])}")

    # Buckets
    buckets = {"<=1x": 0, "1-2x": 0, "2-5x": 0, "5-10x": 0, "10-100x": 0, ">100x": 0}
    for r in ratios:
        if r <= 1: buckets["<=1x"] += 1
        elif r <= 2: buckets["1-2x"] += 1
        elif r <= 5: buckets["2-5x"] += 1
        elif r <= 10: buckets["5-10x"] += 1
        elif r <= 100: buckets["10-100x"] += 1
        else: buckets[">100x"] += 1
    print(f"\n  Bucket distribution:")
    for k, v in buckets.items():
        pct = v / len(ratios) * 100
        bar = "#" * int(pct / 2)
        print(f"    {k:>8s}: {v:4d} ({pct:5.1f}%) {bar}")

no_ev = [e for e in all_ratios if e["evidence_dollars"] == 0]
print(f"\n  Companies with evidence_dollars = 0: {len(no_ev)}")
if no_ev:
    nonzero_ai = [e for e in no_ev if e["ai_index_usd"] > 0]
    print(f"    Of which have ai_index_usd > 0: {len(nonzero_ai)}")
