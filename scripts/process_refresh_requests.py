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
import uuid
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import resend

from ai_opportunity_index.domains import CompanyScore, Notification, PipelineRun, PipelineSubtask, PipelineTask
from ai_opportunity_index.storage.db import (
    complete_pipeline_run,
    create_notification,
    create_pipeline_run,
    get_pending_refresh_requests,
    get_session,
    init_db,
    refresh_latest_scores_view,
    save_company_score,
    save_evidence_batch,
    update_refresh_request_status,
)
from ai_opportunity_index.storage.models import CompanyModel, SubscriberModel
from scripts.score_companies import build_evidence_items, score_single_company

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
                update_refresh_request_status(req.id, "processing")

                # Look up company
                company = session.query(CompanyModel).get(req.company_id)
                if not company:
                    logger.warning("Company ID %d not found, failing request %d", req.company_id, req.id)
                    update_refresh_request_status(req.id, "failed")
                    continue

                # Create pipeline run for scoring
                run_uuid = str(uuid.uuid4())
                pipeline_run = create_pipeline_run(PipelineRun(
                    run_id=run_uuid,
                    task=PipelineTask.SCORE,
                    subtask=PipelineSubtask.ALL,
                    run_type="refresh_request",
                    status="running",
                    tickers_requested=[company.ticker],
                ))

                # Score the company
                now = datetime.utcnow()
                result = score_single_company(company)

                if result:
                    opp = result["opportunity"]
                    capture = result["capture"]
                    idx = result["index"]

                    score = CompanyScore(
                        company_id=company.id,
                        pipeline_run_id=pipeline_run.id,
                        revenue_opp_score=opp.get("revenue_opportunity"),
                        cost_opp_score=opp.get("cost_opportunity"),
                        composite_opp_score=opp["composite_opportunity"],
                        filing_nlp_score=capture.get("filing_nlp_score"),
                        product_score=capture.get("product_score"),
                        composite_real_score=capture["composite_realization"],
                        cost_capture_score=capture["cost_capture"],
                        revenue_capture_score=capture["revenue_capture"],
                        general_investment_score=capture["general_investment"],
                        cost_roi=idx.get("cost_roi"),
                        revenue_roi=idx.get("revenue_roi"),
                        combined_roi=idx.get("combined_roi"),
                        opportunity=idx["opportunity"],
                        realization=idx["realization"],
                        quadrant=idx["quadrant"],
                        quadrant_label=idx["quadrant_label"],
                        flags=result.get("flags", []),
                        data_as_of=now,
                        scored_at=now,
                    )
                    save_company_score(score)

                    evidence_items = build_evidence_items(
                        company_id=company.id,
                        pipeline_run_id=pipeline_run.id,
                        opp_scores=opp,
                        evidence=result.get("evidence", {}),
                        classified_outputs=result.get("classified_outputs"),
                    )
                    if evidence_items:
                        save_evidence_batch(evidence_items)

                    complete_pipeline_run(run_uuid, status="completed", tickers_succeeded=1)
                    update_refresh_request_status(req.id, "completed", pipeline_run_id=pipeline_run.id)

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
                    complete_pipeline_run(run_uuid, status="failed")
                    update_refresh_request_status(req.id, "failed")
                    logger.warning("Scoring returned None for %s", company.ticker)

            except Exception as e:
                logger.error("Failed to process refresh request %d: %s", req.id, e)
                update_refresh_request_status(req.id, "failed")

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
