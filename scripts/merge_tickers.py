#!/usr/bin/env python3
"""Merge child share-class tickers into a canonical parent ticker.

Links child tickers (e.g. GOOG) to their canonical parent (e.g. GOOGL),
moves evidence/financials/evidence_groups to the parent, and deactivates
the child records.

Usage:
    python scripts/merge_tickers.py --canonical GOOGL --children GOOG
    python scripts/merge_tickers.py --canonical BRK.B --children BRK.A
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import select, update

from ai_opportunity_index.storage.db import get_session, init_db
from ai_opportunity_index.storage.models import (
    CompanyModel,
    EvidenceGroupModel,
    EvidenceModel,
    FinancialObservationModel,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def merge_tickers(canonical_ticker: str, child_tickers: list[str], dry_run: bool = False):
    """Link child tickers to canonical parent and merge their data."""
    init_db()
    session = get_session()
    try:
        # Look up canonical company
        parent = session.execute(
            select(CompanyModel).where(CompanyModel.ticker == canonical_ticker.upper())
        ).scalar_one_or_none()
        if not parent:
            logger.error("Canonical ticker %s not found in database", canonical_ticker)
            return

        # Look up child companies
        children = []
        for ct in child_tickers:
            child = session.execute(
                select(CompanyModel).where(CompanyModel.ticker == ct.upper())
            ).scalar_one_or_none()
            if not child:
                logger.error("Child ticker %s not found in database", ct)
                return
            if child.id == parent.id:
                logger.error("Child ticker %s is the same as canonical", ct)
                return
            children.append(child)

        child_ids = [c.id for c in children]
        logger.info(
            "Merging %s (ids=%s) → %s (id=%d)",
            [c.ticker for c in children], child_ids, parent.ticker, parent.id,
        )

        # 1. Set relationships
        parent.child_ticker_refs = child_ids
        for child in children:
            child.canonical_company_id = parent.id
        logger.info("Set canonical_company_id on children, child_ticker_refs on parent")

        # 2. Move evidence: skip if same source_url already exists on parent
        for child in children:
            child_evidence = session.execute(
                select(EvidenceModel).where(EvidenceModel.company_id == child.id)
            ).scalars().all()

            # Get existing source_urls on parent to dedupe
            parent_source_urls = set(
                row[0] for row in session.execute(
                    select(EvidenceModel.source_url)
                    .where(EvidenceModel.company_id == parent.id)
                    .where(EvidenceModel.source_url.isnot(None))
                ).all()
            )

            moved = 0
            skipped = 0
            for ev in child_evidence:
                if ev.source_url and ev.source_url in parent_source_urls:
                    skipped += 1
                    continue
                ev.company_id = parent.id
                moved += 1
                if ev.source_url:
                    parent_source_urls.add(ev.source_url)

            logger.info(
                "  %s evidence: %d moved, %d skipped (duplicate source_url)",
                child.ticker, moved, skipped,
            )

        # 3. Move evidence_groups
        for child in children:
            child_groups = session.execute(
                select(EvidenceGroupModel).where(EvidenceGroupModel.company_id == child.id)
            ).scalars().all()
            for group in child_groups:
                group.company_id = parent.id
            logger.info("  %s evidence_groups: %d moved", child.ticker, len(child_groups))

        # 4. Move financial_observations: skip dupes by metric+source_datetime
        for child in children:
            child_obs = session.execute(
                select(FinancialObservationModel)
                .where(FinancialObservationModel.company_id == child.id)
            ).scalars().all()

            # Existing parent metric+datetime combos
            parent_metric_dates = set(
                (row[0], row[1]) for row in session.execute(
                    select(
                        FinancialObservationModel.metric,
                        FinancialObservationModel.source_datetime,
                    ).where(FinancialObservationModel.company_id == parent.id)
                ).all()
            )

            moved = 0
            skipped = 0
            for obs in child_obs:
                key = (obs.metric, obs.source_datetime)
                if key in parent_metric_dates:
                    skipped += 1
                    continue
                obs.company_id = parent.id
                moved += 1
                parent_metric_dates.add(key)

            logger.info(
                "  %s financial_observations: %d moved, %d skipped (duplicate metric+date)",
                child.ticker, moved, skipped,
            )

        # 5. Deactivate children
        for child in children:
            child.is_active = False
        logger.info("Deactivated child tickers: %s", [c.ticker for c in children])

        if dry_run:
            logger.info("DRY RUN — rolling back changes")
            session.rollback()
        else:
            session.commit()
            logger.info("Merge committed successfully")

        # Summary
        logger.info("--- Summary ---")
        logger.info("Canonical: %s (id=%d)", parent.ticker, parent.id)
        logger.info("Children:  %s", [(c.ticker, c.id) for c in children])
        logger.info("child_ticker_refs: %s", parent.child_ticker_refs)
        logger.info(
            "Next steps: re-run valuation and scoring for %s to reflect combined evidence",
            parent.ticker,
        )

    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def main():
    parser = argparse.ArgumentParser(description="Merge child share-class tickers into canonical parent")
    parser.add_argument("--canonical", required=True, help="Canonical (parent) ticker, e.g. GOOGL")
    parser.add_argument("--children", nargs="+", required=True, help="Child ticker(s), e.g. GOOG")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without committing")
    args = parser.parse_args()

    merge_tickers(args.canonical, args.children, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
