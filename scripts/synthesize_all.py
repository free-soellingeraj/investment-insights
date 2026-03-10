#!/usr/bin/env python3
"""Run project synthesis for all companies with evidence groups but no projects."""

import asyncio
import logging
import sys

import psycopg2

from ai_opportunity_index.scoring.project_synthesis import synthesize_projects
from ai_opportunity_index.storage.repositories import (
    ValuationRepository,
    get_async_session,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def get_candidates(min_groups: int = 5, limit: int = 20):
    """Find companies with evidence groups but no investment projects."""
    conn = psycopg2.connect(dbname="ai_opportunity_index")
    cur = conn.cursor()
    cur.execute("""
        SELECT c.id, c.ticker, c.company_name, c.sector,
               COUNT(eg.id) as group_count
        FROM companies c
        JOIN evidence_groups eg ON eg.company_id = c.id
        LEFT JOIN investment_projects ip ON ip.company_id = c.id
        WHERE ip.id IS NULL
        GROUP BY c.id, c.ticker, c.company_name, c.sector
        HAVING COUNT(eg.id) >= %s
        ORDER BY COUNT(eg.id) DESC
        LIMIT %s
    """, (min_groups, limit))
    rows = cur.fetchall()
    conn.close()
    return rows


async def run_synthesis_for_company(company_id, ticker, company_name, sector):
    """Run synthesis for a single company."""
    async with get_async_session() as session:
        val_repo = ValuationRepository(session)
        groups = await val_repo.get_evidence_groups_for_company(company_id)
        valuations = await val_repo.get_final_valuations_for_company(company_id)

        if not groups:
            logger.info("No evidence groups for %s, skipping", ticker)
            return 0

        logger.info(
            "Synthesizing %s: %d groups, %d valuations",
            ticker, len(groups), len(valuations),
        )

        projects = await synthesize_projects(
            company_id=company_id,
            company_name=company_name or ticker,
            ticker=ticker,
            sector=sector or "Technology",
            revenue=0,
            groups=groups,
            valuations=valuations,
        )

        if projects:
            await val_repo.save_investment_projects(projects)
            logger.info("Saved %d projects for %s", len(projects), ticker)
        else:
            logger.warning("No projects synthesized for %s", ticker)

        return len(projects)


async def main():
    min_groups = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 15

    candidates = get_candidates(min_groups=min_groups, limit=limit)
    logger.info("Found %d candidates with >= %d evidence groups", len(candidates), min_groups)

    total_projects = 0
    for company_id, ticker, company_name, sector, group_count in candidates:
        logger.info("─── %s (%s) ─── %d groups", ticker, company_name, group_count)
        try:
            n = await run_synthesis_for_company(company_id, ticker, company_name, sector)
            total_projects += n
        except Exception as e:
            logger.error("Failed for %s: %s", ticker, e, exc_info=True)
        # Brief pause between API calls
        await asyncio.sleep(1)

    logger.info("Done. Synthesized %d total projects across %d companies", total_projects, len(candidates))


if __name__ == "__main__":
    asyncio.run(main())
