"""Dashboard overview API — returns rich network stats for the main dashboard."""

from datetime import datetime, timezone

from litestar import get
from sqlalchemy import text

from ai_opportunity_index.storage.db import get_session


@get("/api/dashboard/stats")
async def dashboard_stats() -> dict:
    """Return high-level network statistics for the dashboard overview."""
    s = get_session()
    try:
        def scalar(q: str):
            return s.execute(text(q)).scalar()

        def rows(q: str):
            return [dict(r._mapping) for r in s.execute(text(q)).fetchall()]

        # Core counts
        total_companies = scalar("SELECT COUNT(*) FROM companies")
        scored_companies = scalar("SELECT COUNT(DISTINCT company_id) FROM company_scores")
        evidence_groups = scalar("SELECT COUNT(*) FROM evidence_groups")
        valuations = scalar("SELECT COUNT(*) FROM valuations")
        projects = scalar("SELECT COUNT(*) FROM investment_projects")
        passages = scalar("SELECT COUNT(*) FROM evidence_group_passages")
        pipeline_runs = scalar("SELECT COUNT(*) FROM pipeline_runs")

        # Quadrant distribution
        quadrants = rows("""
            SELECT quadrant_label as label, COUNT(*) as count
            FROM company_scores
            WHERE quadrant_label IS NOT NULL
            GROUP BY quadrant_label
            ORDER BY count DESC
        """)

        # Target dimension distribution
        dimensions = rows("""
            SELECT target_dimension as dimension, COUNT(*) as count
            FROM evidence_groups
            WHERE target_dimension IS NOT NULL
            GROUP BY target_dimension
            ORDER BY count DESC
        """)

        # Source type distribution
        source_types = rows("""
            SELECT source_type, COUNT(*) as count
            FROM evidence_group_passages
            GROUP BY source_type
            ORDER BY count DESC
        """)

        # Score freshness buckets
        freshness_row = s.execute(text("""
            SELECT
                COUNT(*) FILTER (WHERE scored_at > NOW() - interval '1 hour') as last_hour,
                COUNT(*) FILTER (WHERE scored_at > NOW() - interval '1 day') as last_day,
                COUNT(*) FILTER (WHERE scored_at > NOW() - interval '7 days') as last_week,
                COUNT(*) as total
            FROM company_scores
        """)).fetchone()
        freshness = dict(freshness_row._mapping) if freshness_row else {}

        # Top companies by AI Index USD (deduplicated, sensible range)
        top_companies = rows("""
            SELECT DISTINCT ON (c.ticker)
                c.ticker, c.company_name, c.sector,
                cs.ai_index_usd, cs.opportunity, cs.realization,
                cs.quadrant_label, cs.combined_rank, cs.scored_at
            FROM company_scores cs
            JOIN companies c ON c.id = cs.company_id
            WHERE cs.ai_index_usd > 0 AND cs.ai_index_usd < 1e11 AND c.ticker IS NOT NULL
            ORDER BY c.ticker, cs.scored_at DESC
        """)
        # Sort by ai_index_usd desc and take top 25
        top_companies.sort(key=lambda r: r.get("ai_index_usd") or 0, reverse=True)
        top_companies = top_companies[:25]

        # Pipeline activity
        pipeline_activity = rows("""
            SELECT status, COUNT(*) as count
            FROM pipeline_runs
            GROUP BY status
            ORDER BY count DESC
        """)

        recent_runs = rows("""
            SELECT id, status, started_at, completed_at,
                   tickers_requested[1:3] as sample_tickers
            FROM pipeline_runs
            ORDER BY id DESC
            LIMIT 5
        """)
        # Serialize datetimes
        for run in recent_runs:
            for k in ("started_at", "completed_at"):
                if run.get(k) and hasattr(run[k], "isoformat"):
                    run[k] = run[k].isoformat()

        # Sector breakdown (top 15)
        sectors = rows("""
            SELECT c.sector, COUNT(DISTINCT c.id) as companies,
                   COUNT(DISTINCT cs.id) as scores,
                   ROUND(AVG(cs.ai_index_usd)::numeric, 0) as avg_ai_usd
            FROM companies c
            LEFT JOIN company_scores cs ON cs.company_id = c.id
            WHERE c.sector IS NOT NULL AND c.sector != ''
            GROUP BY c.sector
            ORDER BY companies DESC
            LIMIT 15
        """)
        # Convert Decimal to float
        for s_row in sectors:
            if s_row.get("avg_ai_usd") is not None:
                s_row["avg_ai_usd"] = float(s_row["avg_ai_usd"])

        # Score distribution histogram (opportunity and realization)
        score_dist = rows("""
            SELECT
                WIDTH_BUCKET(opportunity, 0, 1, 10) as opp_bucket,
                WIDTH_BUCKET(realization, 0, 1, 10) as real_bucket,
                COUNT(*) as count
            FROM company_scores
            GROUP BY opp_bucket, real_bucket
            ORDER BY opp_bucket, real_bucket
        """)

        # Network total dollar value
        total_ai_usd = scalar("""
            SELECT ROUND(SUM(ai_index_usd)::numeric, 0)
            FROM (
                SELECT DISTINCT ON (company_id) ai_index_usd
                FROM company_scores
                WHERE ai_index_usd > 0 AND ai_index_usd < 1e11
                ORDER BY company_id, scored_at DESC
            ) sub
        """)

        return {
            "counts": {
                "companies": total_companies,
                "scored": scored_companies,
                "evidence_groups": evidence_groups,
                "valuations": valuations,
                "projects": projects,
                "passages": passages,
                "pipeline_runs": pipeline_runs,
            },
            "total_ai_usd": float(total_ai_usd) if total_ai_usd else 0,
            "quadrants": quadrants,
            "dimensions": dimensions,
            "source_types": source_types,
            "freshness": freshness,
            "top_companies": top_companies,
            "pipeline_activity": pipeline_activity,
            "recent_runs": recent_runs,
            "sectors": sectors,
            "score_distribution": score_dist,
        }
    finally:
        s.close()
