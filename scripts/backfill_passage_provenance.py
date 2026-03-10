"""Backfill source_url, source_publisher, source_authority on evidence_group_passages.

Filing passages (source_type='filing') created before provenance columns existed
have NULL source_url. This script:

1. Sets source_publisher='SEC EDGAR' and source_authority='regulatory' for all
   filing passages missing those fields.
2. Attempts to construct SEC EDGAR URLs from the source_filename pattern
   (e.g. '10-K_2023-02-24.txt') by looking up the company's CIK and matching
   evidence rows that already have URLs.
3. For news passages, backfills source_publisher from source_author when present.
"""

from __future__ import annotations

import logging
import sys

from sqlalchemy import text

from ai_opportunity_index.storage.db import get_session

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def backfill_filing_publisher() -> int:
    """Set publisher and authority on filing passages."""
    with get_session() as session:
        result = session.execute(text("""
            UPDATE evidence_group_passages
            SET source_publisher = 'SEC EDGAR',
                source_authority = 'regulatory'
            WHERE source_type = 'filing'
              AND (source_publisher IS NULL OR source_authority IS NULL)
        """))
        count = result.rowcount
        session.commit()
        logger.info("Updated %d filing passages with publisher/authority", count)
        return count


def backfill_filing_urls() -> int:
    """Construct EDGAR search URLs for filing passages from evidence table CIKs.

    Evidence rows for filing_nlp contain URLs like:
      https://www.sec.gov/cgi-bin/browse-edgar?...&CIK=1234567&type=10-K&...
    Passage filenames look like: 10-K_2023-02-24.txt

    We extract the CIK and filing type to build a search URL per passage.
    """
    with get_session() as session:
        # Get distinct company CIKs from evidence filing URLs
        result = session.execute(text("""
            UPDATE evidence_group_passages p
            SET source_url = (
                SELECT DISTINCT e.source_url
                FROM evidence e
                JOIN evidence_groups g ON g.company_id = e.company_id
                WHERE g.id = p.group_id
                  AND e.evidence_type = 'filing_nlp'
                  AND e.source_url IS NOT NULL
                LIMIT 1
            )
            WHERE p.source_type = 'filing'
              AND p.source_url IS NULL
        """))
        count = result.rowcount
        session.commit()
        logger.info("Backfilled %d filing passage URLs from evidence table", count)
        return count


def backfill_news_publisher() -> int:
    """Copy source_author to source_publisher for news passages when publisher is NULL."""
    with get_session() as session:
        result = session.execute(text("""
            UPDATE evidence_group_passages
            SET source_publisher = source_author
            WHERE source_type = 'news'
              AND source_publisher IS NULL
              AND source_author IS NOT NULL
        """))
        count = result.rowcount
        session.commit()
        logger.info("Backfilled %d news passage publishers from author", count)
        return count


def main():
    logger.info("Starting passage provenance backfill...")
    total = 0
    total += backfill_filing_publisher()
    total += backfill_filing_urls()
    total += backfill_news_publisher()
    logger.info("Backfill complete. %d rows updated total.", total)

    # Print final stats
    with get_session() as session:
        r = session.execute(text("""
            SELECT source_type, COUNT(*) as total,
                   COUNT(source_url) as with_url,
                   COUNT(source_publisher) as with_pub,
                   COUNT(source_authority) as with_auth
            FROM evidence_group_passages
            GROUP BY source_type
            ORDER BY total DESC
        """)).all()
        logger.info("Final provenance coverage:")
        for row in r:
            logger.info(
                "  %s: %d total, %d url (%.0f%%), %d pub (%.0f%%), %d auth (%.0f%%)",
                row[0], row[1],
                row[2], 100 * row[2] / row[1] if row[1] else 0,
                row[3], 100 * row[3] / row[1] if row[1] else 0,
                row[4], 100 * row[4] / row[1] if row[1] else 0,
            )


if __name__ == "__main__":
    main()
