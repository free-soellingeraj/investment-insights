#!/usr/bin/env python3
"""One-time backfill: populate companies.blog_url from discovered_links JSON cache.

Usage:
    python scripts/backfill_blog_url.py [--dry-run]
"""
import argparse
import json
import sys
from pathlib import Path

# Allow running from repo root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ai_opportunity_index.config import RAW_DIR
from ai_opportunity_index.storage.db import get_session
from ai_opportunity_index.storage.models import CompanyModel


def main():
    parser = argparse.ArgumentParser(description="Backfill blog_url from JSON cache")
    parser.add_argument("--dry-run", action="store_true", help="Print changes without committing")
    args = parser.parse_args()

    discovered_links_dir = RAW_DIR / "discovered_links"
    if not discovered_links_dir.exists():
        print("No discovered_links directory found — nothing to backfill.")
        return

    session = get_session()
    try:
        updated = 0
        skipped = 0
        for json_file in sorted(discovered_links_dir.glob("*.json")):
            try:
                data = json.loads(json_file.read_text())
            except Exception as e:
                print(f"  SKIP {json_file.name}: {e}")
                skipped += 1
                continue

            blog_url = data.get("blog_url")
            if not blog_url:
                continue

            ticker = json_file.stem.upper()
            company = session.query(CompanyModel).filter(
                CompanyModel.ticker == ticker
            ).first()
            if not company:
                print(f"  SKIP {ticker}: not in DB")
                skipped += 1
                continue

            if company.blog_url:
                continue  # already populated

            company.blog_url = blog_url
            updated += 1
            if args.dry_run:
                print(f"  WOULD SET {ticker}.blog_url = {blog_url}")

        if args.dry_run:
            print(f"\nDry run: {updated} companies would be updated, {skipped} skipped.")
            session.rollback()
        else:
            session.commit()
            print(f"Backfill complete: {updated} companies updated, {skipped} skipped.")
    finally:
        session.close()


if __name__ == "__main__":
    main()
