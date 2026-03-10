#!/usr/bin/env python3
"""Process pending refresh requests.

Cloud Run Job that:
1. Queries refresh_requests WHERE status = 'pending'
2. For each: creates scoring run, scores the company, updates status
3. Creates notification when done
4. Sends notification via Resend
"""

import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import resend

from ai_opportunity_index.domains import Notification, RefreshStatus
from ai_opportunity_index.pipeline import PipelineController, PipelineRequest, TriggerSource
from ai_opportunity_index.storage.db import (
    create_notification,
    get_pending_refresh_requests,
    get_session,
    init_db,
    refresh_latest_scores_view,
    update_refresh_request_status,
)
from ai_opportunity_index.storage.models import CompanyModel, SubscriberModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

RESEND_API_KEY = os.environ.get("RESEND_API_KEY")
FROM_EMAIL = os.environ.get("FROM_EMAIL", "hello@winonaquantitative.com")
BASE_URL = os.environ.get("BASE_URL", "http://localhost:8080")


def send_notification_email(email: str, ticker: str, dashboard_url: str):
    """Send refresh-complete notification."""
    if not RESEND_API_KEY:
        logger.warning("Resend not configured — skipping notification to %s", email)
        return

    try:
        resend.api_key = RESEND_API_KEY
        resend.Emails.send({
            "from": f"Winona Quantitative Research <{FROM_EMAIL}>",
            "to": [email],
            "subject": f"Refresh Complete: {ticker}",
            "html": (
                "<div style='font-family:sans-serif;max-width:600px;margin:0 auto;padding:20px'>"
                f"<h1 style='color:#1a1a2e'>Refresh Complete: {ticker}</h1>"
                f"<p>The data refresh you requested for <strong>{ticker}</strong> has been completed.</p>"
                f"<p><a href='{dashboard_url}' style='display:inline-block;background:#6366f1;"
                "color:white;padding:12px 24px;border-radius:8px;text-decoration:none;"
                "font-weight:600'>View Updated Scores</a></p>"
                "<p style='color:#999;font-size:12px'>Winona Quantitative Research</p>"
                "</div>"
            ),
        })
        logger.info("Notification email sent to %s for %s", email, ticker)
    except Exception as e:
        logger.error("Failed to send notification to %s: %s", email, e)


def main():
    init_db()

    pending = get_pending_refresh_requests(limit=20)
    if not pending:
        logger.info("No pending refresh requests.")
        return

    logger.info("Processing %d pending refresh requests", len(pending))

    session = get_session()
    try:
        for req in pending:
            try:
                # Mark as processing
                update_refresh_request_status(req.id, RefreshStatus.PROCESSING)

                # Look up company
                company = session.query(CompanyModel).get(req.company_id)
                if not company:
                    logger.warning("Company ID %d not found, failing request %d", req.company_id, req.id)
                    update_refresh_request_status(req.id, RefreshStatus.FAILED)
                    continue

                # Run extract + value + score via PipelineController
                ticker_str = company.ticker or company.slug
                pipeline_request = PipelineRequest(
                    tickers=[ticker_str],
                    stages={"extract_unified", "value_evidence", "score"},
                    source=TriggerSource.REFRESH_REQUEST,
                    max_concurrency=1,
                    llm_concurrency=5,
                    include_inactive=True,
                )
                results = PipelineController.run_sync(pipeline_request)

                failures = [r for r in results if not r.success and not r.skipped]
                if not failures:
                    update_refresh_request_status(req.id, RefreshStatus.COMPLETED)

                    # Create and send notification
                    subscriber = session.query(SubscriberModel).get(req.subscriber_id)
                    if subscriber:
                        create_notification(Notification(
                            subscriber_id=req.subscriber_id,
                            notification_type="refresh_complete",
                            channel="email",
                            subject=f"Refresh Complete: {company.ticker}",
                            body=f"Data refresh for {company.ticker} ({company.company_name}) has been completed.",
                            payload={"ticker": company.ticker, "request_id": req.id},
                        ))

                        dashboard_url = f"{BASE_URL}/dashboard?token={subscriber.access_token}"
                        send_notification_email(subscriber.email, company.ticker, dashboard_url)

                    logger.info("Refresh complete for %s (request %d)", company.ticker, req.id)
                else:
                    update_refresh_request_status(req.id, RefreshStatus.FAILED)
                    logger.warning("Pipeline failed for %s: %s", company.ticker,
                                   [r.error for r in failures])

            except Exception as e:
                logger.error("Failed to process refresh request %d: %s", req.id, e)
                update_refresh_request_status(req.id, RefreshStatus.FAILED)

        # Refresh materialized view — retry once on failure since CONCURRENTLY
        # can fail due to lock contention.  Dashboard reads from this view so
        # a stale view causes score mismatches vs the detail page.
        for attempt in range(2):
            try:
                refresh_latest_scores_view()
                break
            except Exception as e:
                if attempt == 0:
                    logger.warning("Materialized view refresh failed (attempt 1), retrying: %s", e)
                else:
                    logger.error(
                        "Materialized view refresh FAILED after 2 attempts: %s. "
                        "Dashboard will show stale scores until manually refreshed.",
                        e,
                    )

    finally:
        session.close()

    logger.info("Done processing refresh requests.")


if __name__ == "__main__":
    main()
