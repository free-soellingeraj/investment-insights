"""Litestar web application — landing page, dashboard, Stripe, API endpoints."""

import logging
from pathlib import Path
from typing import Any

import resend
import stripe
from litestar import Litestar, Request, get, post, put
from litestar.response import File, Redirect, Template
from litestar.static_files import create_static_files_router
from litestar.status_codes import HTTP_400_BAD_REQUEST, HTTP_403_FORBIDDEN, HTTP_404_NOT_FOUND, HTTP_500_INTERNAL_SERVER_ERROR, HTTP_503_SERVICE_UNAVAILABLE

from ai_opportunity_index.config import PROCESSED_DIR
from ai_opportunity_index.domains import AIOpportunityEvidence, CompanyRecord, CompanyUpdate, PipelineSubtask, PipelineTask, RefreshRequest, RunStatus
from ai_opportunity_index.storage.db import (
    create_notification,
    create_refresh_request,
    create_subscriber,
    get_ai_index_rank,
    get_company_by_slug,
    get_company_by_ticker,
    get_company_detail,
    get_company_valuation_detail,
    get_evidence_for_company,
    get_industry_peers,
    get_latest_scores,
    get_session,
    get_subscriber_by_email,
    get_subscriber_by_token,
    init_db,
    update_subscriber_status,
)
from ai_opportunity_index.domains import Notification
from web.pipeline_controller import PipelineAPIController
from web.config import (
    ADMIN_EMAIL,
    BASE_URL,
    FROM_EMAIL,
    RESEND_API_KEY,
    STRIPE_PRICE_ID,
    STRIPE_SECRET_KEY,
    STRIPE_WEBHOOK_SECRET,
)

logger = logging.getLogger(__name__)

WEB_DIR = Path(__file__).resolve().parent
LANDING_DIR = WEB_DIR / "landing"
ASSETS_DIR = LANDING_DIR / "assets"


# ── Helpers ───────────────────────────────────────────────────────────────


def _html(content: str, status_code: int = 200) -> dict:
    """Return an HTML response dict."""
    from litestar.response import Response
    return Response(content=content, media_type="text/html", status_code=status_code)


def send_welcome_email(email: str, dashboard_url: str):
    """Send a welcome/confirmation email to a new subscriber."""
    if not RESEND_API_KEY:
        logger.warning("Resend not configured — skipping welcome email to %s", email)
        return

    try:
        resend.Emails.send({
            "from": f"Winona Quantitative Research <{FROM_EMAIL}>",
            "to": [email],
            "subject": "Welcome to the AI Opportunity Index",
            "html": (
                "<div style='font-family:sans-serif;max-width:600px;margin:0 auto;padding:20px'>"
                "<h1 style='color:#1a1a2e'>Welcome to the AI Opportunity Index</h1>"
                "<p>Thank you for subscribing! Your access is now active.</p>"
                "<p>Access your dashboard anytime using the link below:</p>"
                f"<p><a href='{dashboard_url}' style='display:inline-block;background:#6366f1;"
                "color:white;padding:12px 24px;border-radius:8px;text-decoration:none;"
                "font-weight:600'>Open Dashboard</a></p>"
                "<p style='margin-top:24px'>What you get:</p>"
                "<ul>"
                "<li>AI Opportunity scores for 10,000+ public companies</li>"
                "<li>Backtested index performance vs S&amp;P 500</li>"
                "<li>Company-level detail pages with scoring breakdowns</li>"
                "<li>Investment memos and research briefs</li>"
                "</ul>"
                "<p style='margin-top:24px;color:#666;font-size:13px'>"
                "Questions? Reply to this email or reach us at hello@winonaquantitative.com</p>"
                "<p style='color:#999;font-size:12px'>Winona Quantitative Research</p>"
                "</div>"
            ),
        })
        logger.info("Welcome email sent to %s", email)
    except Exception as e:
        logger.error("Failed to send welcome email to %s: %s", email, e)


# ── Pipeline Status ──────────────────────────────────────────────────────


@get("/api/status")
async def api_status() -> dict:
    """Pipeline status: collection progress, scoring progress, costs."""
    import json
    from ai_opportunity_index.storage.models import (
        CompanyModel,
        CompanyScoreModel,
        EvidenceModel,
        FinancialObservationModel,
        PipelineRunModel,
    )

    session = get_session()
    try:
        from sqlalchemy import func, distinct, case, cast, String as SAString

        total_companies = session.query(func.count(CompanyModel.id)).scalar() or 0

        # ── Per-source coverage ──────────────────────────────────────────
        sources = []

        # 1. SEC EDGAR — company universe
        sec_count = total_companies
        sec_latest = session.query(func.max(CompanyModel.created_at)).scalar()
        sources.append({
            "name": "SEC EDGAR (Universe)",
            "phase": "collection",
            "companies": sec_count,
            "records": sec_count,
            "latest_date": sec_latest.isoformat() if sec_latest else None,
            "cost": "free",
        })

        # 2. Yahoo Finance — financials
        yf_companies = session.query(
            func.count(distinct(FinancialObservationModel.company_id))
        ).filter(FinancialObservationModel.source_name == "yahoo_finance").scalar() or 0
        yf_records = session.query(
            func.count(FinancialObservationModel.id)
        ).filter(FinancialObservationModel.source_name == "yahoo_finance").scalar() or 0
        yf_latest = session.query(
            func.max(FinancialObservationModel.created_at)
        ).filter(FinancialObservationModel.source_name == "yahoo_finance").scalar()
        yf_by_metric = dict(
            session.query(
                FinancialObservationModel.metric,
                func.count(FinancialObservationModel.id),
            ).filter(
                FinancialObservationModel.source_name == "yahoo_finance"
            ).group_by(FinancialObservationModel.metric).all()
        )
        sources.append({
            "name": "Yahoo Finance",
            "phase": "collection",
            "companies": yf_companies,
            "records": yf_records,
            "latest_date": yf_latest.isoformat() if yf_latest else None,
            "detail": yf_by_metric,
            "cost": "free",
        })

        # 3. Yahoo Finance — sector/industry enrichment
        sector_count = session.query(func.count(CompanyModel.id)).filter(
            CompanyModel.sector.isnot(None)
        ).scalar() or 0
        sources.append({
            "name": "Yahoo Finance (Sector)",
            "phase": "collection",
            "companies": sector_count,
            "records": sector_count,
            "latest_date": yf_latest.isoformat() if yf_latest else None,
            "cost": "free",
        })

        # 4. SEC EDGAR — filings on disk
        from ai_opportunity_index.config import RAW_DIR
        filings_dir = RAW_DIR / "filings"
        filing_companies = 0
        total_filings = 0
        filing_detail = {}
        if filings_dir.exists():
            for company_dir in filings_dir.iterdir():
                if company_dir.is_dir():
                    files = list(company_dir.glob("*.txt"))
                    if files:
                        filing_companies += 1
                        total_filings += len(files)
                        for f in files:
                            ftype = f.stem.split("_")[0] if "_" in f.stem else "other"
                            filing_detail[ftype] = filing_detail.get(ftype, 0) + 1
        sources.append({
            "name": "SEC EDGAR (Filings)",
            "phase": "collection",
            "companies": filing_companies,
            "records": total_filings,
            "latest_date": None,
            "detail": filing_detail,
            "cost": "free",
        })

        # 4b. GitHub signals on disk
        github_dir = RAW_DIR / "github"
        github_companies = 0
        if github_dir.exists():
            github_companies = sum(1 for f in github_dir.glob("*.json"))
        sources.append({
            "name": "GitHub",
            "phase": "collection",
            "companies": github_companies,
            "records": github_companies,
            "latest_date": None,
            "cost": "free",
        })

        # 4c. Analyst data on disk
        analyst_dir = RAW_DIR / "analysts"
        analyst_companies = 0
        if analyst_dir.exists():
            analyst_companies = sum(1 for f in analyst_dir.glob("*.json"))
        sources.append({
            "name": "Yahoo Finance (Analysts)",
            "phase": "collection",
            "companies": analyst_companies,
            "records": analyst_companies,
            "latest_date": None,
            "cost": "free",
        })

        # 4d. Discovered Links (from DB)
        from ai_opportunity_index.storage.models import CompanyModel as CM
        discovered_links_companies = session.query(func.count(CM.id)).filter(
            (CM.github_url.isnot(None)) | (CM.careers_url.isnot(None))
            | (CM.ir_url.isnot(None)) | (CM.blog_url.isnot(None))
        ).scalar()
        discovered_links_with_blog = session.query(func.count(CM.id)).filter(
            CM.blog_url.isnot(None)
        ).scalar()
        sources.append({
            "name": "Discovered Links",
            "phase": "collection",
            "companies": discovered_links_companies,
            "records": discovered_links_companies,
            "latest_date": None,
            "detail": {"with_blog_url": discovered_links_with_blog},
            "cost": "~$0.0001/company (Gemini Flash)",
        })

        # 4e. Web enrichment on disk
        web_enrichment_dir = RAW_DIR / "web_enrichment"
        web_enrichment_companies = 0
        if web_enrichment_dir.exists():
            web_enrichment_companies = sum(1 for f in web_enrichment_dir.glob("*.json"))
        sources.append({
            "name": "Web Enrichment",
            "phase": "collection",
            "companies": web_enrichment_companies,
            "records": web_enrichment_companies,
            "latest_date": None,
            "cost": "~$0.0001/company (Gemini Flash)",
        })

        # Reference for daily cache loop
        discovered_links_dir = RAW_DIR / "discovered_links"

        # 4f. News cache on disk
        news_dir = RAW_DIR / "news"
        news_companies = 0
        if news_dir.exists():
            news_companies = sum(1 for f in news_dir.glob("*.json"))
        sources.append({
            "name": "News Cache",
            "phase": "collection",
            "companies": news_companies,
            "records": news_companies,
            "latest_date": None,
            "cost": "free",
        })


        # 5-10. Evidence-based sources (from scoring phase)
        # Keys must match evidence_type values in score_companies.py
        evidence_sources = {
            "cost_opportunity": ("Cost Opportunity", "value", "free"),
            "revenue_opportunity": ("Revenue Opportunity", "value", "free"),
            "filing_nlp": ("Filing NLP", "extraction", "~$0.003/filing (Gemini Flash)"),
            "product": ("Products (GNews)", "extraction", "~$0.001/article (Gemini Flash)"),
            "web_enrichment": ("Web Enrichment (Extract)", "extraction", "~$0.0001/company (Gemini Flash)"),
            "job": ("Job Postings", "extraction", "free"),
        }
        ev_rows = session.query(
            EvidenceModel.evidence_type,
            func.count(distinct(EvidenceModel.company_id)),
            func.count(EvidenceModel.id),
            func.max(EvidenceModel.observed_at),
        ).group_by(EvidenceModel.evidence_type).all()
        ev_map = {row[0]: row for row in ev_rows}

        for ev_type, (label, phase, cost_label) in evidence_sources.items():
            row = ev_map.get(ev_type)
            sources.append({
                "name": label,
                "phase": phase,
                "companies": row[1] if row else 0,
                "records": row[2] if row else 0,
                "latest_date": row[3].isoformat() if row and row[3] else None,
                "cost": cost_label,
            })

        # Also capture any evidence types not in our map
        for ev_type, companies, records, latest in ev_rows:
            if ev_type not in evidence_sources:
                sources.append({
                    "name": f"Evidence: {ev_type}",
                    "phase": "scoring",
                    "companies": companies,
                    "records": records,
                    "latest_date": latest.isoformat() if latest else None,
                    "cost": "unknown",
                })

        # 10. Scoring completeness
        companies_scored = session.query(
            func.count(distinct(CompanyScoreModel.company_id))
        ).scalar() or 0
        score_latest = session.query(
            func.max(CompanyScoreModel.scored_at)
        ).scalar()
        sources.append({
            "name": "Composite Scores",
            "phase": "scoring",
            "companies": companies_scored,
            "records": session.query(func.count(CompanyScoreModel.id)).scalar() or 0,
            "latest_date": score_latest.isoformat() if score_latest else None,
            "cost": "n/a",
        })

        # ── Pipeline runs (grouped by task/subtask) ────────────────────────
        pipeline_runs = session.query(PipelineRunModel).order_by(
            PipelineRunModel.started_at.desc()
        ).limit(50).all()

        def _run_dict(r):
            return {
                "run_id": str(r.run_id),
                "task": r.task,
                "subtask": r.subtask,
                "run_type": r.run_type,
                "status": r.status,
                "tickers_succeeded": r.tickers_succeeded,
                "tickers_failed": r.tickers_failed,
                "parent_run_id": r.parent_run_id,
                "parameters": r.parameters or {},
                "started_at": r.started_at.isoformat() if r.started_at else None,
                "completed_at": r.completed_at.isoformat() if r.completed_at else None,
                "error_message": r.error_message,
            }

        # Group by task, keep latest 5 per task
        runs_by_task: dict[str, list] = {}
        for r in pipeline_runs:
            task_list = runs_by_task.setdefault(r.task, [])
            if len(task_list) < 5:
                task_list.append(_run_dict(r))

        # Per-task summary: total runs, last run time, any currently running
        task_summary = []
        for task in PipelineTask:
            task_runs = [r for r in pipeline_runs if r.task == task.value]
            running = [r for r in task_runs if r.status == RunStatus.RUNNING]
            completed = [r for r in task_runs if r.status == RunStatus.COMPLETED]
            latest = task_runs[0] if task_runs else None
            task_summary.append({
                "task": task.value,
                "total_runs": session.query(func.count(PipelineRunModel.id)).filter(
                    PipelineRunModel.task == task.value
                ).scalar() or 0,
                "currently_running": len(running),
                "last_completed": (
                    completed[0].completed_at.isoformat()
                    if completed and completed[0].completed_at else None
                ),
                "last_status": latest.status if latest else None,
                "last_error": latest.error_message if latest and latest.status == RunStatus.FAILED else None,
            })

        # Flatten for recent_runs: all runs sorted by time
        runs = [_run_dict(r) for r in pipeline_runs[:20]]

        # ── Cost summary ─────────────────────────────────────────────────
        from ai_opportunity_index.config import DATA_DIR
        import json
        cost_summary = None
        cost_path = DATA_DIR / "cost_summary.json"
        if cost_path.exists():
            try:
                summaries = json.loads(cost_path.read_text())
                if isinstance(summaries, list) and summaries:
                    cost_summary = summaries[-1]
            except Exception:
                pass

        # ── Daily time-series per source ──────────────────────────────────
        from sqlalchemy import text
        daily = {}

        # Financial observations by day and source
        fin_daily = session.execute(text("""
            SELECT source_name,
                   DATE(created_at) AS day,
                   COUNT(DISTINCT company_id) AS companies,
                   COUNT(*) AS records
            FROM financial_observations
            GROUP BY source_name, DATE(created_at)
            ORDER BY day DESC
        """)).fetchall()
        for source_name, day, companies, records in fin_daily:
            day_str = day.isoformat()
            label = f"Yahoo Finance" if source_name == "yahoo_finance" else f"Financials ({source_name})"
            daily.setdefault(label, {})[day_str] = {"companies": companies, "records": records}

        # Evidence by day and type
        ev_daily = session.execute(text("""
            SELECT evidence_type,
                   DATE(observed_at) AS day,
                   COUNT(DISTINCT company_id) AS companies,
                   COUNT(*) AS records
            FROM evidence
            GROUP BY evidence_type, DATE(observed_at)
            ORDER BY day DESC
        """)).fetchall()
        ev_labels = {
            "filing_nlp": "Filing NLP",
            "cost_opportunity": "Cost Opportunity",
            "revenue_opportunity": "Revenue Opportunity",
            "product": "Products (GNews)",
        }
        for ev_type, day, companies, records in ev_daily:
            day_str = day.isoformat()
            label = ev_labels.get(ev_type, ev_type)
            daily.setdefault(label, {})[day_str] = {"companies": companies, "records": records}

        # Scores by day
        score_daily = session.execute(text("""
            SELECT DATE(scored_at) AS day,
                   COUNT(DISTINCT company_id) AS companies,
                   COUNT(*) AS records
            FROM company_scores
            GROUP BY DATE(scored_at)
            ORDER BY day DESC
        """)).fetchall()
        for day, companies, records in score_daily:
            daily.setdefault("Composite Scores", {})[day.isoformat()] = {"companies": companies, "records": records}

        # Companies seeded by day
        seed_daily = session.execute(text("""
            SELECT DATE(created_at) AS day,
                   COUNT(*) AS companies
            FROM companies
            GROUP BY DATE(created_at)
            ORDER BY day DESC
        """)).fetchall()
        for day, companies in seed_daily:
            daily.setdefault("SEC EDGAR (Universe)", {})[day.isoformat()] = {"companies": companies, "records": companies}

        # Filings on disk — group by file modification date
        filings_daily = {}
        if filings_dir.exists():
            import os
            from datetime import date as date_cls
            for company_dir in filings_dir.iterdir():
                if company_dir.is_dir():
                    for f in company_dir.glob("*.txt"):
                        try:
                            mtime = date_cls.fromtimestamp(os.path.getmtime(f))
                            day_str = mtime.isoformat()
                            if day_str not in filings_daily:
                                filings_daily[day_str] = {"companies": set(), "records": 0}
                            filings_daily[day_str]["companies"].add(company_dir.name)
                            filings_daily[day_str]["records"] += 1
                        except Exception:
                            pass
        for day_str, val in filings_daily.items():
            daily.setdefault("SEC EDGAR (Filings)", {})[day_str] = {
                "companies": len(val["companies"]),
                "records": val["records"],
            }

        # GitHub cache files by modification date
        for cache_label, cache_dir in [("GitHub", github_dir), ("Yahoo Finance (Analysts)", analyst_dir), ("Discovered Links", discovered_links_dir), ("Web Enrichment", web_enrichment_dir), ("News Cache", news_dir)]:
            cache_daily = {}
            if cache_dir.exists():
                for f in cache_dir.glob("*.json"):
                    try:
                        mtime = date_cls.fromtimestamp(os.path.getmtime(f))
                        day_str = mtime.isoformat()
                        if day_str not in cache_daily:
                            cache_daily[day_str] = 0
                        cache_daily[day_str] += 1
                    except Exception:
                        pass
            for day_str, count in cache_daily.items():
                daily.setdefault(cache_label, {})[day_str] = {
                    "companies": count, "records": count,
                }

        # ── Pipeline stages (computed from sources) ────────────────────
        src_map = {s["name"]: s for s in sources}

        # Extract: companies with at least one extraction evidence type
        extract_types = ["filing_nlp", "product", "web_enrichment"]
        companies_extracted = session.query(
            func.count(distinct(EvidenceModel.company_id))
        ).filter(
            EvidenceModel.evidence_type.in_(extract_types)
        ).scalar() or 0

        # Fully scored: companies that have BOTH opportunity evidence AND
        # capture evidence (i.e. went through extraction before scoring).
        # Companies scored without extraction data have all-zero capture
        # scores, which are misleading.
        companies_fully_scored = session.query(
            func.count(distinct(CompanyScoreModel.company_id))
        ).filter(
            CompanyScoreModel.company_id.in_(
                session.query(distinct(EvidenceModel.company_id)).filter(
                    EvidenceModel.evidence_type.in_(extract_types)
                )
            )
        ).scalar() or 0

        pipeline_stages = [
            {
                "name": "Collect",
                "color": "#34d399",
                "complete": min(
                    src_map.get("Yahoo Finance", {}).get("companies", 0),
                    src_map.get("SEC EDGAR (Filings)", {}).get("companies", 0),
                ),
                "sub_sources": [
                    "SEC EDGAR (Universe)",
                    "Yahoo Finance",
                    "Yahoo Finance (Sector)",
                    "SEC EDGAR (Filings)",
                    "Yahoo Finance (Analysts)",
                    "GitHub",
                    "Discovered Links",
                    "Web Enrichment",
                    "News Cache",
                ],
            },
            {
                "name": "Extract",
                "color": "#38bdf8",
                "complete": companies_extracted,
                "sub_sources": [
                    "Filing NLP",
                    "Products (GNews)",
                    "Web Enrichment (Extract)",
                    "Job Postings",
                ],
            },
            {
                "name": "Value",
                "color": "#818cf8",
                "complete": min(
                    src_map.get("Cost Opportunity", {}).get("companies", 0),
                    src_map.get("Revenue Opportunity", {}).get("companies", 0),
                ),
                "sub_sources": ["Cost Opportunity", "Revenue Opportunity"],
            },
            {
                "name": "Score",
                "color": "#fbbf24",
                "complete": companies_fully_scored,
                "detail": {
                    "fully_scored": companies_fully_scored,
                    "scored_total": companies_scored,
                    "scored_without_extraction": companies_scored - companies_fully_scored,
                },
                "sub_sources": ["Composite Scores"],
            },
        ]

        return {
            "total_companies": total_companies,
            "sources": sources,
            "daily": daily,
            "pipeline": pipeline_stages,
            "scoring": {
                "companies_scored": companies_scored,
                "recent_runs": runs,
                "runs_by_task": runs_by_task,
                "task_summary": task_summary,
            },
            "cost": cost_summary,
        }
    finally:
        session.close()


@get("/api/status/extractors")
async def api_status_extractors() -> dict:
    """Detailed metadata about every extractor in the pipeline."""
    import json as _json
    from ai_opportunity_index.config import RAW_DIR
    from ai_opportunity_index.storage.models import EvidenceModel
    from sqlalchemy import func, distinct

    session = get_session()
    try:
        # Get per evidence_type company counts
        ev_counts = dict(
            session.query(
                EvidenceModel.evidence_type,
                func.count(distinct(EvidenceModel.company_id)),
            ).group_by(EvidenceModel.evidence_type).all()
        )

        # Count web enrichment cache files
        we_dir = RAW_DIR / "web_enrichment"
        we_count = sum(1 for f in we_dir.glob("*.json")) if we_dir.exists() else 0
        # Count how many have each section populated
        we_careers = we_ir = we_blog = 0
        if we_dir.exists():
            for f in we_dir.glob("*.json"):
                try:
                    d = _json.loads(f.read_text())
                    if d.get("careers") and d["careers"].get("evidence_items"):
                        we_careers += 1
                    if d.get("investor_relations") and d["investor_relations"].get("evidence_items"):
                        we_ir += 1
                    if d.get("blog") and d["blog"].get("evidence_items"):
                        we_blog += 1
                except Exception:
                    pass

        extractors = [
            {
                "id": "filing_nlp",
                "name": "Filing NLP",
                "evidence_type": "filing_nlp",
                "companies_extracted": ev_counts.get("filing_nlp", 0),
                "method": "llm",
                "model": "gemini-2.5-flash",
                "input_source": "SEC EDGAR 10-K/10-Q filings",
                "input_path": "data/raw/filings/{TICKER}/*.txt",
                "description": "LLM-based extraction from SEC filings using Gemini Flash. Reads filing text and identifies AI-related passages with structured classification into cost/revenue/general dimensions and planned/invested/realized stages.",
                "prompt": "You are an expert financial analyst specializing in AI technology adoption.\n\nExtract specific evidence passages about AI initiatives from this SEC filing.\n\nFor each AI-related passage found, identify:\n1. The exact quote (max 300 chars)\n2. Whether it relates to cost reduction, revenue generation, or general AI investment\n3. Whether it is planned, invested, or realized\n4. Confidence level (0.0-1.0) that this is genuine AI activity vs. boilerplate\n\nReturn: passage_text, target_dimension, capture_stage, confidence, reasoning",
                "classification": {
                    "target_dimension": "LLM classifies directly: \"cost\" (automation/efficiency), \"revenue\" (AI products/services), or \"general\" (strategy/R&D)",
                    "capture_stage": "LLM classifies directly: \"planned\" (intentions), \"invested\" (actual spending), or \"realized\" (results with metrics)",
                },
                "keywords": None,
                "output_fields": ["passage_text", "target_dimension", "capture_stage", "confidence", "reasoning"],
            },
            {
                "id": "news",
                "name": "News / Products",
                "evidence_type": "product",
                "companies_extracted": ev_counts.get("product", 0),
                "method": "llm",
                "model": "gemini-2.5-flash",
                "input_source": "Google News RSS + SEC EDGAR 8-K EFTS",
                "input_path": "data/raw/news/{TICKER}.json",
                "description": "LLM-based extraction from news articles using Gemini Flash. Identifies AI product launches, partnerships, internal deployments, and strategy announcements with structured classification.",
                "prompt": "You are an expert technology analyst tracking AI product launches and deployments.\n\nIdentify whether this article contains evidence of:\n1. AI product launches — new AI-powered products or features\n2. AI partnerships — collaborations with AI companies\n3. Internal AI deployment — AI used for cost reduction or automation\n4. AI strategy announcements — plans or investments in AI\n\nReturn: passage_text, target_dimension, capture_stage, confidence, reasoning",
                "classification": {
                    "target_dimension": "LLM classifies directly: \"cost\" (internal efficiency), \"revenue\" (products/services), or \"general\"",
                    "capture_stage": "LLM classifies directly: \"planned\", \"invested\", or \"realized\"",
                },
                "keywords": None,
                "output_fields": ["passage_text", "target_dimension", "capture_stage", "confidence", "reasoning"],
            },
            {
                "id": "web_careers",
                "name": "Web Enrichment — Careers",
                "evidence_type": "web_enrichment",
                "companies_extracted": we_careers,
                "method": "llm",
                "model": "gemini-2.5-flash",
                "input_source": "Company careers page (HTTP scrape)",
                "input_path": "data/raw/web_enrichment/{TICKER}.json → careers",
                "description": "Scrapes company careers pages with requests + BeautifulSoup, strips non-content elements, truncates to 15K chars, then sends to Gemini Flash for evidence-oriented extraction. AI/ML hiring is classified as 'invested' in cost (automation roles) or revenue (AI product roles).",
                "prompt": "Identify AI hiring evidence. Classify each role: target_dimension (cost for automation/MLOps roles, revenue for AI product roles, general if unclear), capture_stage (always invested — active hiring = committed spending), confidence (0-1).",
                "classification": {
                    "target_dimension": "LLM classifies directly: 'cost' (automation/MLOps/internal AI roles), 'revenue' (AI product/solutions roles), or 'general'",
                    "capture_stage": "Always 'invested' — active hiring represents committed spending",
                },
                "keywords": None,
                "output_fields": ["evidence_items[].passage_text", "evidence_items[].target_dimension", "evidence_items[].capture_stage", "evidence_items[].confidence", "evidence_items[].reasoning", "page_summary"],
            },
            {
                "id": "web_ir",
                "name": "Web Enrichment — Investor Relations",
                "evidence_type": "web_enrichment",
                "companies_extracted": we_ir,
                "method": "llm",
                "model": "gemini-2.5-flash",
                "input_source": "Company IR page (HTTP scrape)",
                "input_path": "data/raw/web_enrichment/{TICKER}.json → investor_relations",
                "description": "Scrapes company IR pages, extracts visible text, then uses Gemini Flash to identify AI evidence. Strategy mentions → planned; spending announcements → invested; savings/revenue metrics → realized.",
                "prompt": "Extract AI investment evidence from IR content. Classify: target_dimension (cost for efficiency, revenue for AI products, general for strategy), capture_stage (planned for intentions, invested for spending, realized for results), confidence (0-1).",
                "classification": {
                    "target_dimension": "LLM classifies directly: 'cost' (internal AI efficiency), 'revenue' (AI products/services), or 'general' (broad AI strategy)",
                    "capture_stage": "LLM classifies directly: 'planned' (strategy/intentions), 'invested' (announced spending/partnerships), 'realized' (reported savings/revenue metrics)",
                },
                "keywords": None,
                "output_fields": ["evidence_items[].passage_text", "evidence_items[].target_dimension", "evidence_items[].capture_stage", "evidence_items[].confidence", "evidence_items[].reasoning", "page_summary"],
            },
            {
                "id": "web_blog",
                "name": "Web Enrichment — Blog / Product",
                "evidence_type": "web_enrichment",
                "companies_extracted": we_blog,
                "method": "llm",
                "model": "gemini-2.5-flash",
                "input_source": "Company blog / newsroom page (HTTP scrape)",
                "input_path": "data/raw/web_enrichment/{TICKER}.json → blog",
                "description": "Scrapes company blog or newsroom pages, extracts visible text, then uses Gemini Flash for evidence-oriented extraction. AI product launches → revenue invested/realized; AI efficiency posts → cost invested/realized.",
                "prompt": "Extract AI evidence from blog content. Classify: target_dimension (cost for internal automation, revenue for customer-facing AI, general for commentary), capture_stage (planned for upcoming, invested for launched, realized for reported metrics), confidence (0-1).",
                "classification": {
                    "target_dimension": "LLM classifies directly: 'cost' (internal AI automation), 'revenue' (AI products/features for customers), or 'general' (broad AI commentary)",
                    "capture_stage": "LLM classifies directly: 'planned' (announced plans), 'invested' (launched products/tools), 'realized' (adoption metrics/results)",
                },
                "keywords": None,
                "output_fields": ["evidence_items[].passage_text", "evidence_items[].target_dimension", "evidence_items[].capture_stage", "evidence_items[].confidence", "evidence_items[].reasoning", "page_summary"],
            },
            {
                "id": "cost_opportunity",
                "name": "Cost Opportunity",
                "evidence_type": "cost_opportunity",
                "companies_extracted": ev_counts.get("cost_opportunity", 0),
                "method": "formula",
                "model": None,
                "input_source": "BLS occupational data + Microsoft AI applicability scores + Yahoo Finance employee count",
                "input_path": "data/bls_salary_data.csv + data/microsoft_ai_applicability/",
                "description": "Estimates what share of a company's workforce performs tasks that AI could automate or augment. Maps SIC/NAICS → SOC occupation groups, looks up AI applicability per occupation, and scales by employee count.",
                "prompt": None,
                "classification": {
                    "target_dimension": "Always cost — measures AI-automatable workforce share",
                    "capture_stage": "N/A — this is structural opportunity, not company activity",
                },
                "keywords": None,
                "output_fields": ["cost_opportunity", "workforce_roles[].name", "workforce_roles[].ai_applicability", "employee_count", "employee_scaling_factor"],
            },
            {
                "id": "revenue_opportunity",
                "name": "Revenue Opportunity",
                "evidence_type": "revenue_opportunity",
                "companies_extracted": ev_counts.get("revenue_opportunity", 0),
                "method": "formula",
                "model": None,
                "input_source": "BLS occupational data + industry analysis + Yahoo Finance sector/revenue",
                "input_path": "data/bls_salary_data.csv + database (sector, industry, revenue)",
                "description": "Estimates how much AI could enhance or create new revenue streams. For B2B companies, analyzes customer industry AI applicability. For others, uses own workforce AI applicability as a proxy. Applies B2B and AI-industry boosts.",
                "prompt": None,
                "classification": {
                    "target_dimension": "Always revenue — measures AI-addressable revenue potential",
                    "capture_stage": "N/A — this is structural opportunity, not company activity",
                },
                "keywords": None,
                "output_fields": ["revenue_opportunity", "customer_industries[].industry", "customer_industries[].avg_ai_applicability", "is_b2b", "b2b_boost"],
            },
        ]

        return {"extractors": extractors}
    finally:
        session.close()


@get("/api/status/companies")
async def api_status_companies() -> dict:
    """Per-company pipeline completion for the drilldown table."""
    from ai_opportunity_index.storage.models import (
        CompanyModel,
        CompanyScoreModel,
        EvidenceModel,
        FinancialObservationModel,
    )
    from sqlalchemy import func, exists, select, case, literal
    from ai_opportunity_index.config import RAW_DIR

    session = get_session()
    try:
        has_collect = exists(
            select(literal(1)).where(
                FinancialObservationModel.company_id == CompanyModel.id,
                FinancialObservationModel.source_name == "yahoo_finance",
            )
        )
        has_extract = exists(
            select(literal(1)).where(
                EvidenceModel.company_id == CompanyModel.id,
                EvidenceModel.evidence_type == "filing_nlp",
            )
        )
        has_value = exists(
            select(literal(1)).where(
                EvidenceModel.company_id == CompanyModel.id,
                EvidenceModel.evidence_type.in_(["cost_opportunity", "revenue_opportunity"]),
            )
        )
        has_score = exists(
            select(literal(1)).where(
                CompanyScoreModel.company_id == CompanyModel.id,
            )
        )

        rows = session.query(
            CompanyModel.ticker,
            CompanyModel.company_name,
            CompanyModel.github_url,
            CompanyModel.careers_url,
            CompanyModel.ir_url,
            CompanyModel.blog_url,
            case((has_collect, 1), else_=0).label("has_collect"),
            case((has_extract, 1), else_=0).label("has_extract"),
            case((has_value, 1), else_=0).label("has_value"),
            case((has_score, 1), else_=0).label("has_score"),
        ).order_by(CompanyModel.ticker).all()

        # Build per-source cache ticker sets
        def _cache_tickers(subdir):
            d = RAW_DIR / subdir
            return {f.stem for f in d.glob("*.json")} if d.exists() else set()

        news_tickers = _cache_tickers("news")
        github_tickers = _cache_tickers("github")
        analyst_tickers = _cache_tickers("analysts")
        web_enrichment_tickers = _cache_tickers("web_enrichment")

        companies = [
            {
                "ticker": r.ticker,
                "company_name": r.company_name,
                "has_collect": bool(r.has_collect),
                "has_extract": bool(r.has_extract),
                "has_value": bool(r.has_value),
                "has_score": bool(r.has_score),
                "has_github_url": bool(r.github_url),
                "has_careers_url": bool(r.careers_url),
                "has_ir_url": bool(r.ir_url),
                "has_blog_url": bool(r.blog_url),
                "has_news": r.ticker in news_tickers,
                "has_github": r.ticker in github_tickers,
                "has_analyst": r.ticker in analyst_tickers,
                "has_web_enrichment": r.ticker in web_enrichment_tickers,
            }
            for r in rows
        ]
        return {"companies": companies, "count": len(companies)}
    finally:
        session.close()


@get("/api/status/companies/{ticker:str}")
async def api_status_company_detail(ticker: str) -> dict:
    """Detailed pipeline data for a single company."""
    from ai_opportunity_index.storage.models import (
        CompanyModel,
        FinancialObservationModel,
    )
    from ai_opportunity_index.config import RAW_DIR

    session = get_session()
    try:
        company = session.query(CompanyModel).filter(
            CompanyModel.ticker == ticker.upper()
        ).first()
        if not company:
            return {"error": "Company not found"}

        # Collect: financial observations grouped by metric
        fin_rows = session.query(
            FinancialObservationModel.metric,
            FinancialObservationModel.value,
            FinancialObservationModel.value_units,
            FinancialObservationModel.source_name,
            FinancialObservationModel.fiscal_period,
            FinancialObservationModel.created_at,
        ).filter(
            FinancialObservationModel.company_id == company.id
        ).order_by(FinancialObservationModel.metric, FinancialObservationModel.created_at.desc()).all()

        financials = [
            {
                "metric": r.metric,
                "value": r.value,
                "units": r.value_units,
                "source": r.source_name,
                "period": r.fiscal_period,
                "date": r.created_at.isoformat(),
            }
            for r in fin_rows
        ]

        # Collect: filings on disk
        filings_dir = RAW_DIR / "filings" / ticker.upper()
        filings = []
        if filings_dir.exists():
            for f in sorted(filings_dir.glob("*.txt")):
                filings.append({"filename": f.name, "size_kb": round(f.stat().st_size / 1024, 1)})

        # Extract + Value: evidence records grouped by type
        from sqlalchemy import text
        ev_rows = session.execute(text("""
            SELECT evidence_type, evidence_subtype, source_name, source_url,
                   source_date, target_dimension, capture_stage,
                   LEFT(source_excerpt, 200) AS excerpt,
                   payload, observed_at,
                   source_author, source_publisher, source_access_date, source_authority
            FROM evidence
            WHERE company_id = :cid
            ORDER BY evidence_type, observed_at DESC
        """), {"cid": company.id}).fetchall()

        evidence = []
        ev_types_set = set()
        for r in ev_rows:
            ev_types_set.add(r.evidence_type)
            evidence.append({
                "type": r.evidence_type,
                "subtype": r.evidence_subtype,
                "source": r.source_name,
                "url": r.source_url,
                "source_date": r.source_date.isoformat() if r.source_date else None,
                "target": r.target_dimension,
                "stage": r.capture_stage,
                "excerpt": r.excerpt,
                "dollar_usd": r.payload.get("dollar_estimate_usd") if r.payload else None,
                "observed_at": r.observed_at.isoformat(),
                "source_author": r.source_author,
                "source_publisher": r.source_publisher,
                "source_access_date": r.source_access_date.isoformat() if r.source_access_date else None,
                "source_authority": r.source_authority,
            })

        # Score: latest scores
        score_row = session.execute(text("""
            SELECT quadrant, quadrant_label, opportunity, realization,
                   cost_opp_score, revenue_opp_score, cost_capture_score,
                   revenue_capture_score, composite_opp_score, composite_real_score,
                   filing_nlp_score, product_score, github_score, analyst_score,
                   combined_rank, flags, scored_at
            FROM company_scores
            WHERE company_id = :cid
            ORDER BY scored_at DESC LIMIT 1
        """), {"cid": company.id}).fetchone()

        score = None
        if score_row:
            score = {
                "quadrant": score_row.quadrant,
                "quadrant_label": score_row.quadrant_label,
                "opportunity": score_row.opportunity,
                "realization": score_row.realization,
                "cost_opp_score": score_row.cost_opp_score,
                "revenue_opp_score": score_row.revenue_opp_score,
                "cost_capture_score": score_row.cost_capture_score,
                "revenue_capture_score": score_row.revenue_capture_score,
                "composite_opp": score_row.composite_opp_score,
                "composite_real": score_row.composite_real_score,
                "filing_nlp_score": score_row.filing_nlp_score,
                "product_score": score_row.product_score,
                "github_score": score_row.github_score,
                "analyst_score": score_row.analyst_score,
                "rank": score_row.combined_rank,
                "scored_at": score_row.scored_at.isoformat(),
                "flags": list(score_row.flags),
            }

        return {
            "ticker": ticker.upper(),
            "company_name": company.company_name,
            "sector": company.sector,
            "industry": company.industry,
            "financials": financials,
            "filings": filings,
            "evidence": evidence,
            "score": score,
            "summary": {
                "financial_metrics": len(set(r.metric for r in fin_rows)),
                "financial_records": len(fin_rows),
                "filings_count": len(filings),
                "evidence_count": len(evidence),
                "evidence_types": sorted(ev_types_set),
                "has_score": score is not None,
            },
        }
    finally:
        session.close()


@get("/status")
async def status_page() -> Any:
    """Human-readable pipeline status page with pipeline funnel visualization."""
    from litestar.response import Response
    html = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Pipeline Status — AI Opportunity Index</title>
<style>
  * { box-sizing: border-box; }
  body { font-family: system-ui, -apple-system, sans-serif; background: #0f0f1a; color: #e0e0e0; padding: 24px; margin: 0; }
  .container { max-width: 1200px; margin: 0 auto; }
  h1 { color: #818cf8; margin-bottom: 4px; }
  .subtitle { color: #888; margin-bottom: 24px; font-size: 14px; }
  .controls { display: flex; align-items: center; gap: 12px; margin-bottom: 24px; flex-wrap: wrap; }
  .refresh-btn { background: #6366f1; color: white; border: none; padding: 8px 16px; border-radius: 6px; cursor: pointer; font-size: 13px; }
  .refresh-btn:hover { background: #818cf8; }
  .auto-label { color: #888; font-size: 13px; }
  .toggle-group { display: flex; gap: 8px; }
  .toggle-btn { background: #1a1a2e; color: #888; border: 1px solid #2a2a4a; padding: 4px 12px; border-radius: 4px; cursor: pointer; font-size: 12px; }
  .toggle-btn.active { background: #6366f1; color: white; border-color: #6366f1; }
  .last-updated { color: #555; font-size: 11px; }

  /* Nav bar */
  .nav { position: fixed; top: 0; left: 0; right: 0; z-index: 50; background: rgba(15,15,26,0.85); backdrop-filter: blur(8px); border-bottom: 1px solid #1f1f3a; }
  .nav-inner { max-width: 1200px; margin: 0 auto; padding: 12px 24px; display: flex; align-items: center; justify-content: space-between; }
  .nav-left { display: flex; align-items: center; gap: 12px; }
  .nav-logo { width: 30px; height: 30px; background: #6366f1; border-radius: 8px; display: flex; align-items: center; justify-content: center; font-weight: 700; font-size: 12px; color: white; }
  .nav-title { font-size: 15px; font-weight: 600; color: #e0e0e0; }
  .nav-links { display: flex; gap: 16px; }
  .nav-links a { color: #888; font-size: 13px; text-decoration: none; transition: color 0.15s; }
  .nav-links a:hover { color: #e0e0e0; }
  .nav-links a.active { color: #818cf8; }

  /* Pipeline funnel */
  .pipeline-funnel { display: flex; align-items: stretch; gap: 0; margin-bottom: 24px; }
  .pipeline-stage { flex: 1; background: #1a1a2e; border-radius: 12px; padding: 16px 12px; text-align: center; position: relative; min-width: 0; }
  .pipeline-arrow { display: flex; align-items: center; justify-content: center; color: #444; font-size: 20px; padding: 0 8px; flex-shrink: 0; }
  .stage-num { font-size: 11px; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 4px; font-weight: 600; }
  .stage-count { font-size: 20px; font-weight: 700; margin: 4px 0; font-variant-numeric: tabular-nums; white-space: nowrap; }
  .stage-pct { font-size: 14px; color: #aaa; margin-bottom: 8px; font-weight: 600; }
  .stage-bar { height: 8px; background: #2a2a4a; border-radius: 4px; overflow: hidden; }
  .stage-bar-fill { height: 100%; border-radius: 4px; transition: width 0.6s ease; }

  /* Overall progress */
  .overall-wrap { margin-bottom: 24px; }
  .overall-bar { height: 12px; background: #2a2a4a; border-radius: 6px; overflow: hidden; }
  .overall-bar-fill { height: 100%; border-radius: 6px; background: linear-gradient(90deg, #34d399, #38bdf8, #818cf8, #fbbf24); transition: width 0.6s ease; }
  .overall-label { font-size: 14px; color: #aaa; margin-top: 6px; }

  /* Health indicators */
  .health-row { display: flex; gap: 12px; margin-bottom: 24px; flex-wrap: wrap; }
  .health-card { background: #1a1a2e; border-radius: 10px; padding: 14px 18px; flex: 1; min-width: 140px; }
  .health-card .h-label { font-size: 11px; color: #666; text-transform: uppercase; letter-spacing: 0.5px; }
  .health-card .h-val { font-size: 22px; font-weight: 700; margin-top: 2px; }
  .health-card .h-sub { font-size: 11px; color: #555; margin-top: 2px; }

  /* Expandable stage details */
  .stage-detail { background: #1a1a2e; border-radius: 12px; margin-bottom: 8px; overflow: hidden; }
  .stage-detail-header { display: flex; align-items: center; gap: 12px; padding: 14px 18px; cursor: pointer; user-select: none; }
  .stage-detail-header:hover { background: #1e1e35; }
  .stage-detail-toggle { font-size: 12px; color: #666; width: 16px; flex-shrink: 0; transition: transform 0.2s; }
  .stage-detail-toggle.open { transform: rotate(90deg); }
  .stage-detail-name { font-weight: 600; font-size: 14px; white-space: nowrap; }
  .stage-detail-summary { font-size: 13px; color: #aaa; margin-left: auto; white-space: nowrap; }
  .stage-detail-minibar { width: 140px; height: 6px; background: #2a2a4a; border-radius: 3px; overflow: hidden; flex-shrink: 0; margin-left: 8px; }
  .stage-detail-minibar-fill { height: 100%; border-radius: 3px; transition: width 0.4s ease; }
  .stage-detail-pct { font-size: 13px; font-weight: 600; width: 44px; text-align: right; flex-shrink: 0; }
  .stage-detail-body { padding: 0 18px 16px 46px; display: none; }
  .stage-detail-body.open { display: block; }
  .sub-source { display: flex; align-items: center; gap: 10px; margin-bottom: 8px; }
  .sub-source-name { font-size: 13px; color: #ccc; width: 200px; flex-shrink: 0; line-height: 1.4; }
  .sub-source-count { font-size: 13px; color: #aaa; width: 110px; text-align: right; flex-shrink: 0; font-variant-numeric: tabular-nums; white-space: nowrap; }
  .sub-source-bar { flex: 1; height: 6px; background: #2a2a4a; border-radius: 3px; overflow: hidden; min-width: 60px; }
  .sub-source-bar-fill { height: 100%; border-radius: 3px; transition: width 0.4s ease; }
  .sub-source-pct { font-size: 12px; color: #888; width: 44px; text-align: right; flex-shrink: 0; font-variant-numeric: tabular-nums; }
  .missing-link { font-size: 12px; color: #818cf8; cursor: pointer; margin-top: 4px; }
  .missing-link:hover { text-decoration: underline; }

  /* Extractor cards */
  .extractor-grid { display: flex; flex-direction: column; gap: 8px; margin-top: 12px; }
  .extractor-card { background: #12121f; border: 1px solid #2a2a4a; border-radius: 10px; overflow: hidden; }
  .extractor-header { display: flex; align-items: center; gap: 10px; padding: 12px 16px; cursor: pointer; }
  .extractor-header:hover { background: #1a1a30; }
  .extractor-toggle { font-size: 11px; color: #666; width: 14px; flex-shrink: 0; transition: transform 0.2s; }
  .extractor-toggle.open { transform: rotate(90deg); }
  .extractor-name { font-weight: 600; font-size: 13px; color: #e0e0e0; }
  .extractor-badge { font-size: 10px; padding: 2px 8px; border-radius: 4px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; }
  .extractor-badge.keyword { background: #1a2e1a; color: #34d399; }
  .extractor-badge.llm { background: #2a1a3a; color: #c084fc; }
  .extractor-badge.formula { background: #1a2a3a; color: #38bdf8; }
  .extractor-count { margin-left: auto; font-size: 13px; color: #aaa; font-variant-numeric: tabular-nums; }
  .extractor-body { display: none; padding: 0 16px 14px 16px; border-top: 1px solid #1f1f3a; }
  .extractor-body.open { display: block; }
  .ext-section { margin-top: 10px; }
  .ext-label { font-size: 10px; text-transform: uppercase; letter-spacing: 0.5px; color: #666; font-weight: 600; margin-bottom: 4px; }
  .ext-desc { font-size: 12px; color: #aaa; line-height: 1.5; }
  .ext-prompt { font-size: 11px; color: #c084fc; background: #1a1030; border: 1px solid #2a1a4a; border-radius: 6px; padding: 10px 12px; white-space: pre-wrap; line-height: 1.5; font-family: monospace; max-height: 200px; overflow-y: auto; }
  .ext-kw-group { margin-bottom: 6px; }
  .ext-kw-label { font-size: 11px; color: #888; font-weight: 500; margin-bottom: 3px; }
  .ext-kw-list { display: flex; flex-wrap: wrap; gap: 4px; }
  .ext-kw { font-size: 10px; padding: 2px 6px; border-radius: 3px; font-family: monospace; }
  .ext-kw.high { background: #1a3a1a; color: #34d399; }
  .ext-kw.medium { background: #2a2a1a; color: #fbbf24; }
  .ext-kw.low { background: #1a1a2e; color: #818cf8; }
  .ext-kw.default { background: #1a1a2e; color: #aaa; }
  .ext-patterns { font-size: 11px; color: #aaa; }
  .ext-patterns dt { color: #ccc; font-weight: 600; margin-top: 6px; }
  .ext-patterns dd { margin: 2px 0 0 16px; font-style: italic; color: #888; }
  .ext-classification { font-size: 11px; color: #aaa; }
  .ext-classification dt { color: #ccc; font-weight: 600; margin-top: 6px; }
  .ext-classification dd { margin: 2px 0 0 16px; color: #888; }
  .ext-output { display: flex; flex-wrap: wrap; gap: 4px; }
  .ext-field { font-size: 10px; padding: 2px 6px; border-radius: 3px; background: #1a1a2e; color: #a5b4fc; font-family: monospace; }
  .ext-meta-row { display: flex; gap: 16px; margin-top: 8px; flex-wrap: wrap; }
  .ext-meta-item { font-size: 11px; }
  .ext-meta-item .meta-label { color: #666; }
  .ext-meta-item .meta-value { color: #ccc; }
  .tooltip-wrap { position: relative; display: inline-block; }
  .tooltip-wrap .tooltip-text { visibility: hidden; background: #0a0a15; color: #ccc; border: 1px solid #3a3a5a; border-radius: 6px; padding: 8px 12px; font-size: 11px; white-space: pre-wrap; max-width: 400px; position: absolute; bottom: calc(100% + 6px); left: 50%; transform: translateX(-50%); z-index: 20; line-height: 1.4; pointer-events: none; }
  .tooltip-wrap:hover .tooltip-text { visibility: visible; }

  /* Company drilldown */
  .drilldown-wrap { background: #1a1a2e; border-radius: 12px; padding: 20px; margin-bottom: 24px; }
  .drilldown-wrap h2 { color: #a5b4fc; margin: 0 0 12px 0; font-size: 16px; }
  .drilldown-controls { display: flex; gap: 10px; align-items: center; margin-bottom: 12px; flex-wrap: wrap; }
  .drilldown-search { background: #0f0f1a; border: 1px solid #2a2a4a; color: #e0e0e0; padding: 6px 12px; border-radius: 6px; font-size: 13px; width: 200px; }
  .drilldown-search:focus { outline: none; border-color: #6366f1; }
  .filter-btn { background: #0f0f1a; color: #888; border: 1px solid #2a2a4a; padding: 4px 12px; border-radius: 4px; cursor: pointer; font-size: 12px; }
  .filter-btn.active { background: #6366f1; color: white; border-color: #6366f1; }
  .load-btn { background: #6366f1; color: white; border: none; padding: 6px 16px; border-radius: 6px; cursor: pointer; font-size: 13px; }
  .load-btn:hover { background: #818cf8; }
  .load-btn:disabled { background: #333; color: #666; cursor: default; }
  table.company-table { width: 100%; border-collapse: collapse; font-size: 13px; }
  table.company-table th { text-align: left; color: #888; font-weight: 500; padding: 6px 10px; border-bottom: 1px solid #2a2a4a; }
  table.company-table th.stage-col { text-align: center; width: 52px; }
  table.company-table td { padding: 6px 10px; border-bottom: 1px solid #16162a; }
  table.company-table td.stage-cell { text-align: center; font-size: 15px; }
  .check { color: #34d399; }
  .cross { color: #f87171; }
  .company-row { cursor: pointer; }
  .company-row:hover { background: #1e1e35; }
  .detail-row td { padding: 0 !important; border-bottom: 2px solid #2a2a4a; }
  .detail-panel { padding: 16px 20px; background: #12121f; }
  .detail-panel .dp-section { margin-bottom: 14px; }
  .detail-panel .dp-title { font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 6px; }
  .detail-panel table.dp-table { width: 100%; border-collapse: collapse; font-size: 12px; }
  .detail-panel table.dp-table th { text-align: left; color: #666; padding: 3px 8px; border-bottom: 1px solid #1a1a3a; font-weight: 500; }
  .detail-panel table.dp-table td { padding: 3px 8px; border-bottom: 1px solid #16162a; color: #bbb; }
  .detail-panel .dp-empty { color: #555; font-size: 12px; font-style: italic; }
  .detail-panel .dp-loading { color: #888; font-size: 12px; }
  .dp-excerpt { color: #888; font-size: 11px; max-width: 400px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .dp-tag { display: inline-block; padding: 1px 6px; border-radius: 3px; font-size: 10px; font-weight: 600; margin-right: 4px; }
  .dp-tag.cost { background: #1a2e1a; color: #34d399; }
  .dp-tag.revenue { background: #1a1a2e; color: #818cf8; }
  .dp-tag.planned { background: #2a2a1a; color: #fbbf24; }
  .dp-tag.invested { background: #1a2a3a; color: #38bdf8; }
  .dp-tag.realized { background: #1a3a1a; color: #34d399; }

  /* Heatmap grid */
  .heatmap-wrap { background: #1a1a2e; border-radius: 12px; padding: 20px; margin-bottom: 20px; overflow-x: auto; }
  .heatmap-wrap h2 { color: #a5b4fc; margin: 0 0 4px 0; font-size: 16px; }
  .heatmap-wrap .help { color: #666; font-size: 12px; margin-bottom: 12px; }
  .heatmap { border-collapse: collapse; font-size: 12px; width: 100%; }
  .heatmap th { padding: 6px 10px; text-align: center; color: #a5b4fc; font-weight: 600;
    border-bottom: 2px solid #2a2a4a; white-space: nowrap; font-size: 11px; }
  .heatmap th.date-col { text-align: left; color: #888; min-width: 90px; }
  .heatmap td { padding: 5px 8px; text-align: center; border-bottom: 1px solid #16162a;
    font-variant-numeric: tabular-nums; cursor: default; position: relative; }
  .heatmap td.date-cell { text-align: left; color: #aaa; font-weight: 500; white-space: nowrap; font-size: 12px; }
  .heatmap tr:first-child td.date-cell { color: #818cf8; font-weight: 700; }
  .heatmap td .count { font-weight: 600; font-size: 13px; }
  .heatmap td.empty { color: #333; }
  .heatmap .phase-header { font-size: 10px; color: #666; text-transform: uppercase; letter-spacing: 1px; }
  .heatmap .col-total { border-top: 2px solid #2a2a4a; font-weight: 700; color: #a5b4fc; }
  .heat-0 { background: #0f0f1a; }
  .heat-1 { background: #1a1a3a; }
  .heat-2 { background: #1e2550; }
  .heat-3 { background: #233068; }
  .heat-4 { background: #2a3d82; }
  .heat-5 { background: #324a9c; }
  .heat-6 { background: #3b58b5; }
  .heat-7 { background: #4568cc; }
  .heat-8 { background: #5078dd; }
  .heat-9 { background: #5d8aee; }
  .heatmap td:hover .tip { display: block; }
  .tip { display: none; position: absolute; bottom: 100%; left: 50%; transform: translateX(-50%);
    background: #0a0a15; border: 1px solid #3a3a5a; border-radius: 6px; padding: 8px 12px;
    white-space: nowrap; z-index: 10; font-size: 12px; color: #ccc; pointer-events: none; }

  /* Cards */
  .card { background: #1a1a2e; border-radius: 12px; padding: 20px; margin-bottom: 16px; }
  .card h2 { color: #a5b4fc; margin: 0 0 12px 0; font-size: 16px; }
  table.detail { width: 100%; border-collapse: collapse; font-size: 13px; }
  table.detail th { text-align: left; color: #888; font-weight: 500; padding: 6px 8px; border-bottom: 1px solid #2a2a4a; }
  table.detail td { padding: 6px 8px; border-bottom: 1px solid #1a1a3a; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; }
  .badge.completed { background: #064e3b; color: #34d399; }
  .badge.running { background: #3b2f00; color: #fbbf24; }
  .badge.failed { background: #4a1515; color: #f87171; }

  /* Mobile responsive */
  @media (max-width: 768px) {
    .pipeline-funnel { flex-direction: column; gap: 8px; }
    .pipeline-arrow { transform: rotate(90deg); padding: 4px 0; }
    .health-row { flex-direction: column; }
    .sub-source-name { width: 130px; }
    .sub-source-count { width: 90px; }
    .stage-count { font-size: 16px; }
    .drilldown-search { width: 100%; }
  }
</style>
</head>
<body>
<nav class="nav">
  <div class="nav-inner">
    <div class="nav-left">
      <div class="nav-logo">AI</div>
      <span class="nav-title">AI Opportunity Index</span>
    </div>
    <div class="nav-links">
      <a href="/dashboard">Dashboard</a>
      <a href="/status" class="active">Pipeline Status</a>
    </div>
  </div>
</nav>
<div class="container" style="padding-top: 64px;">
<h1>Pipeline Status</h1>
<p class="subtitle">Collect &rarr; Extract &rarr; Value &rarr; Score</p>

<div class="controls">
  <button class="refresh-btn" onclick="loadStatus()">Refresh</button>
  <label class="auto-label"><input type="checkbox" id="auto-toggle" checked> Auto 10s</label>
  <span style="flex:1"></span>
  <div class="toggle-group">
    <button class="toggle-btn active" data-mode="companies" onclick="setMode('companies')">Companies</button>
    <button class="toggle-btn" data-mode="records" onclick="setMode('records')">Records</button>
  </div>
  <span class="last-updated" id="updated"></span>
</div>

<div id="pipeline-funnel"></div>
<div id="overall-progress"></div>
<div id="health-indicators"></div>
<div id="stage-details"></div>
<div id="company-drilldown">
  <div class="drilldown-wrap">
    <h2>Company Pipeline Status</h2>
    <p style="color:#888;font-size:13px;margin-bottom:12px">Click to load per-company pipeline details with source and evidence breakdown.</p>
    <button class="load-btn" onclick="loadCompanies()">Load Company Details</button>
  </div>
</div>
<div id="heatmap"></div>
<div id="extras"></div>
</div>

<script>
let timer, DATA, MODE = 'companies';
let COMPANIES = null, COMPANY_FILTER = 'all', COMPANY_SEARCH = '';
let EXPANDED_STAGES = {0: true};  // first stage expanded by default
let EXTRACTORS = null;
let EXPANDED_EXTRACTORS = {};
const fmt = n => n != null ? n.toLocaleString() : '—';
const pct = (n, total) => total > 0 ? Math.round(n / total * 100) : 0;

const SOURCE_META = {
  'SEC EDGAR (Universe)': { phase: '1-collect', short: 'Universe' },
  'Yahoo Finance': { phase: '1-collect', short: 'Yahoo Fin' },
  'Yahoo Finance (Sector)': { phase: '1-collect', short: 'Sector' },
  'SEC EDGAR (Filings)': { phase: '1-collect', short: 'Filings' },
  'Yahoo Finance (Analysts)': { phase: '1-collect', short: 'Analysts' },
  'GitHub': { phase: '1-collect', short: 'GitHub' },
  'Discovered Links': { phase: '1-collect', short: 'Links' },
  'Web Enrichment': { phase: '1-collect', short: 'Web' },
  'News Cache': { phase: '1-collect', short: 'News' },
  'Filing NLP': { phase: '2-extract', short: 'Filing NLP' },
  'Products (GNews)': { phase: '2-extract', short: 'News' },
  'Cost Opportunity': { phase: '3-value', short: 'Cost Opp $' },
  'Revenue Opportunity': { phase: '3-value', short: 'Rev Opp $' },
  'Composite Scores': { phase: '4-score', short: 'Scores' },
};
const SOURCE_ORDER = Object.keys(SOURCE_META);

// Which input source feeds each extractor/processor
const SOURCE_INPUT = {
  'SEC EDGAR (Universe)': 'SEC EDGAR XBRL feed',
  'Yahoo Finance': 'Yahoo Finance API',
  'Yahoo Finance (Sector)': 'Yahoo Finance API',
  'SEC EDGAR (Filings)': 'SEC EDGAR full-text downloads',
  'Yahoo Finance (Analysts)': 'Yahoo Finance API (yfinance)',
  'GitHub': 'GitHub REST API',
  'Discovered Links': 'Homepage scrape + Gemini Flash',
  'Web Enrichment': 'Web scrape + Gemini Flash (careers/IR/blog pages)',
  'News Cache': 'Google News RSS + SEC EDGAR 8-K EFTS',
  'Filing NLP': 'SEC EDGAR (Filings)',
  'Products (GNews)': 'GNews API',
  'Cost Opportunity': 'Yahoo Finance + BLS labor data',
  'Revenue Opportunity': 'Yahoo Finance + industry analysis',
  'Composite Scores': 'All evidence + valuations',
};

function setMode(m) {
  MODE = m;
  document.querySelectorAll('.toggle-btn').forEach(b => b.classList.toggle('active', b.dataset.mode === m));
  if (DATA) render(DATA);
}

function heatClass(val, maxVal) {
  if (!val || val === 0) return 'heat-0';
  const ratio = Math.min(val / Math.max(maxVal, 1), 1);
  return 'heat-' + Math.min(Math.ceil(ratio * 9), 9);
}

function toggleStage(i) {
  EXPANDED_STAGES[i] = !EXPANDED_STAGES[i];
  if (DATA) renderStageDetails(DATA);
}

// ── Pipeline Funnel ──
function renderPipeline(d) {
  const stages = d.pipeline || [];
  const total = d.total_companies || 1;
  let h = '<div class="pipeline-funnel">';
  stages.forEach((s, i) => {
    const p = pct(s.complete, total);
    if (i > 0) h += '<div class="pipeline-arrow">&#x2192;</div>';
    h += `<div class="pipeline-stage">
      <div class="stage-num" style="color:${s.color}">${i+1}. ${s.name}</div>
      <div class="stage-count" style="color:${s.color}">${fmt(s.complete)}/${fmt(total)}</div>
      <div class="stage-pct">${p}%</div>
      <div class="stage-bar"><div class="stage-bar-fill" style="width:${p}%;background:${s.color}"></div></div>
    </div>`;
  });
  h += '</div>';
  document.getElementById('pipeline-funnel').innerHTML = h;
}

// ── Overall Progress ──
function renderOverall(d) {
  const stages = d.pipeline || [];
  const total = d.total_companies || 1;
  // Average completion across all pipeline stages — scores are less
  // valuable when upstream data is incomplete
  const stagePcts = stages.map(s => pct(s.complete, total));
  const avg = stagePcts.length > 0
    ? Math.round(stagePcts.reduce((a, b) => a + b, 0) / stagePcts.length)
    : 0;
  const breakdown = stages.map((s, i) => `${s.name} ${stagePcts[i]}%`).join(' · ');
  document.getElementById('overall-progress').innerHTML = `
    <div class="overall-wrap">
      <div class="overall-bar"><div class="overall-bar-fill" style="width:${avg}%"></div></div>
      <div class="overall-label">Overall: <strong style="color:#e0e0e0">${avg}%</strong> — avg of ${breakdown}</div>
    </div>`;
}

// ── Health Indicators ──
function renderHealth(d) {
  const stages = d.pipeline || [];
  const total = d.total_companies || 1;
  const scored = d.scoring?.companies_scored || 0;
  const costTotal = d.cost?.total_estimated_cost_usd;

  // Bottleneck = stage with lowest %
  let bottleneck = stages[0] || {name:'—',complete:0};
  let minPct = 100;
  stages.forEach(s => {
    const p = pct(s.complete, total);
    if (p < minPct) { minPct = p; bottleneck = s; }
  });

  const unscored = total - scored;
  const stagePcts = stages.map(s => pct(s.complete, total));
  const avgPct = stagePcts.length > 0
    ? Math.round(stagePcts.reduce((a, b) => a + b, 0) / stagePcts.length) : 0;

  document.getElementById('health-indicators').innerHTML = `
    <div class="health-row">
      <div class="health-card">
        <div class="h-label">Bottleneck</div>
        <div class="h-val" style="color:${bottleneck.color || '#f87171'}">${bottleneck.name}: ${minPct}%</div>
        <div class="h-sub">Lowest pipeline stage</div>
      </div>
      <div class="health-card">
        <div class="h-label">Unscored</div>
        <div class="h-val" style="color:#f87171">${fmt(unscored)}</div>
        <div class="h-sub">companies without scores</div>
      </div>
      <div class="health-card">
        <div class="h-label">Data Quality</div>
        <div class="h-val" style="color:${avgPct >= 75 ? '#34d399' : avgPct >= 40 ? '#fbbf24' : '#f87171'}">${avgPct}%</div>
        <div class="h-sub">avg stage completion</div>
      </div>
      <div class="health-card">
        <div class="h-label">Est. Cost</div>
        <div class="h-val" style="color:#fbbf24">$${costTotal != null ? costTotal.toFixed(4) : '0'}</div>
        <div class="h-sub">total pipeline cost</div>
      </div>
    </div>`;
}

// ── Extractor card helpers ──
function toggleExtractor(id) {
  EXPANDED_EXTRACTORS[id] = !EXPANDED_EXTRACTORS[id];
  if (DATA) renderStageDetails(DATA);
}

function escHtml(s) { return s ? s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;') : ''; }

function renderKeywords(kw) {
  if (!kw) return '';
  let h = '<div class="ext-section"><div class="ext-label">Keywords</div>';
  const tierColors = { high_weight: 'high', medium_weight: 'medium', low_weight: 'low', ai_filter: 'high', cost_domain: 'medium', revenue_domain: 'default', ai_filter: 'high', revenue_roles: 'default', cost_roles: 'medium' };
  const tierLabels = { high_weight: 'High weight (3)', medium_weight: 'Medium weight (2)', low_weight: 'Low weight (1)', ai_filter: 'AI filter', cost_domain: 'Cost domain', revenue_domain: 'Revenue domain', revenue_roles: 'Revenue roles', cost_roles: 'Cost roles' };
  for (const [tier, words] of Object.entries(kw)) {
    const cls = tierColors[tier] || 'default';
    const label = tierLabels[tier] || tier.replace(/_/g, ' ');
    h += `<div class="ext-kw-group"><div class="ext-kw-label">${label}</div><div class="ext-kw-list">`;
    for (const w of words) h += `<span class="ext-kw ${cls}">${escHtml(w)}</span>`;
    h += '</div></div>';
  }
  h += '</div>';
  return h;
}

function renderPatterns(patterns) {
  if (!patterns) return '';
  let h = '<div class="ext-section"><div class="ext-label">Regex Pattern Sets</div><dl class="ext-patterns">';
  for (const [name, desc] of Object.entries(patterns)) {
    const label = name.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
    h += `<dt>${label}</dt><dd>${escHtml(desc)}</dd>`;
  }
  h += '</dl></div>';
  return h;
}

function renderClassification(cls) {
  if (!cls) return '';
  let h = '<div class="ext-section"><div class="ext-label">Classification Logic</div><dl class="ext-classification">';
  if (cls.target_dimension) h += `<dt>Target Dimension</dt><dd>${escHtml(cls.target_dimension)}</dd>`;
  if (cls.capture_stage) h += `<dt>Capture Stage</dt><dd>${escHtml(cls.capture_stage)}</dd>`;
  h += '</dl></div>';
  return h;
}

function renderExtractorCard(ext, stageColor) {
  const isOpen = !!EXPANDED_EXTRACTORS[ext.id];
  const badgeCls = ext.method === 'llm' ? 'llm' : ext.method === 'formula' ? 'formula' : 'keyword';
  const badgeLabel = ext.method === 'llm' ? 'LLM' : ext.method === 'formula' ? 'Formula' : 'Keyword';

  let h = `<div class="extractor-card">
    <div class="extractor-header" onclick="toggleExtractor('${ext.id}')">
      <span class="extractor-toggle ${isOpen ? 'open' : ''}">&#x25B6;</span>
      <span class="extractor-name">${escHtml(ext.name)}</span>
      <span class="extractor-badge ${badgeCls}">${badgeLabel}</span>
      ${ext.model ? `<span style="font-size:10px;color:#888;font-family:monospace">${ext.model}</span>` : ''}
      <span class="extractor-count" style="color:${stageColor}">${fmt(ext.companies_extracted)} companies</span>
    </div>
    <div class="extractor-body ${isOpen ? 'open' : ''}">`;

  // Description
  h += `<div class="ext-section"><div class="ext-label">What it does</div><div class="ext-desc">${escHtml(ext.description)}</div></div>`;

  // Meta row: input, path, evidence_type
  h += `<div class="ext-meta-row">
    <div class="ext-meta-item"><span class="meta-label">Input: </span><span class="meta-value">${escHtml(ext.input_source)}</span></div>
    <div class="ext-meta-item"><span class="meta-label">Path: </span><span class="meta-value" style="font-family:monospace;font-size:11px">${escHtml(ext.input_path)}</span></div>
    <div class="ext-meta-item"><span class="meta-label">Evidence type: </span><span class="meta-value" style="font-family:monospace">${escHtml(ext.evidence_type)}</span></div>
  </div>`;

  // Prompt (for LLM extractors)
  if (ext.prompt) {
    h += `<div class="ext-section"><div class="ext-label">LLM Prompt</div><div class="ext-prompt">${escHtml(ext.prompt)}</div></div>`;
  }

  // Keywords
  h += renderKeywords(ext.keywords);

  // Patterns
  h += renderPatterns(ext.patterns);

  // Classification
  h += renderClassification(ext.classification);

  // Output fields
  if (ext.output_fields && ext.output_fields.length > 0) {
    h += '<div class="ext-section"><div class="ext-label">Output Fields</div><div class="ext-output">';
    for (const f of ext.output_fields) h += `<span class="ext-field">${escHtml(f)}</span>`;
    h += '</div></div>';
  }

  h += '</div></div>';
  return h;
}

// Map stage names to extractor groups
const STAGE_EXTRACTORS = {
  'Extract': ['filing_nlp', 'news', 'web_careers', 'web_ir', 'web_blog'],
  'Value': ['cost_opportunity', 'revenue_opportunity'],
};

// ── Expandable Stage Details ──
function renderStageDetails(d) {
  const stages = d.pipeline || [];
  const sources = d.sources || [];
  const total = d.total_companies || 1;
  const srcMap = {};
  sources.forEach(s => srcMap[s.name] = s);

  let h = '';
  stages.forEach((stage, i) => {
    const p = pct(stage.complete, total);
    const isOpen = !!EXPANDED_STAGES[i];
    const stageExtractorIds = STAGE_EXTRACTORS[stage.name];
    const hasExtractors = stageExtractorIds && EXTRACTORS && EXTRACTORS.length > 0;
    const extractorCount = hasExtractors ? EXTRACTORS.filter(e => stageExtractorIds.includes(e.id)).length : 0;
    const summaryText = hasExtractors
      ? `${extractorCount} extractor${extractorCount > 1 ? 's' : ''} · ${stage.sub_sources.length} source${stage.sub_sources.length > 1 ? 's' : ''}`
      : `${stage.sub_sources.length} source${stage.sub_sources.length > 1 ? 's' : ''}`;

    h += `<div class="stage-detail">
      <div class="stage-detail-header" onclick="toggleStage(${i})">
        <span class="stage-detail-toggle ${isOpen ? 'open' : ''}">&#x25B6;</span>
        <span class="stage-detail-name" style="color:${stage.color}">${i+1}. ${stage.name} (${fmt(stage.complete)}/${fmt(total)})</span>
        <span class="stage-detail-summary">${summaryText}</span>
        <div class="stage-detail-minibar"><div class="stage-detail-minibar-fill" style="width:${p}%;background:${stage.color}"></div></div>
        <span class="stage-detail-pct" style="color:${stage.color}">${p}%</span>
      </div>
      <div class="stage-detail-body ${isOpen ? 'open' : ''}">`;

    // Render sub-sources (collection progress bars)
    for (const subName of stage.sub_sources) {
      const src = srcMap[subName] || {};
      const subCount = src.companies || 0;
      const subPct = pct(subCount, total);
      const inputSrc = SOURCE_INPUT[subName] || '';
      h += `<div class="sub-source">
        <div class="sub-source-name">${subName}<br><span style="font-size:11px;color:#555">${inputSrc}</span></div>
        <span class="sub-source-count">${fmt(subCount)}/${fmt(total)}</span>
        <div class="sub-source-bar"><div class="sub-source-bar-fill" style="width:${subPct}%;background:${stage.color}"></div></div>
        <span class="sub-source-pct">${subPct}%</span>
      </div>`;
    }

    // Render extractor cards for Extract and Value stages
    if (hasExtractors) {
      const stageExtractors = EXTRACTORS.filter(e => stageExtractorIds.includes(e.id));
      if (stageExtractors.length > 0) {
        h += `<div style="margin-top:16px;padding-top:12px;border-top:1px solid #1f1f3a">
          <div style="font-size:11px;text-transform:uppercase;letter-spacing:0.5px;color:#666;font-weight:600;margin-bottom:8px">Extractors — click to expand details, prompts &amp; keywords</div>
          <div class="extractor-grid">`;
        for (const ext of stageExtractors) {
          h += renderExtractorCard(ext, stage.color);
        }
        h += '</div></div>';
      }
    }

    const missing = total - stage.complete;
    if (missing > 0) {
      h += `<div class="missing-link" onclick="loadCompanies()">${missing} companies missing. Load details &rarr;</div>`;
    }
    h += '</div></div>';
  });
  document.getElementById('stage-details').innerHTML = h;
}

// ── Company Drilldown ──
async function loadCompanies() {
  const btn = document.querySelector('.load-btn');
  if (btn) { btn.disabled = true; btn.textContent = 'Loading...'; }
  try {
    const resp = await fetch('/api/status/companies');
    if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${resp.statusText}`);
    const data = await resp.json();
    if (!data.companies) throw new Error('API response missing "companies" key: ' + JSON.stringify(Object.keys(data)));
    COMPANIES = data.companies;
    renderCompanyTable();
  } catch (e) {
    document.getElementById('company-drilldown').innerHTML =
      `<div class="drilldown-wrap"><p style="color:#f87171">Error loading companies: ${e.message}</p></div>`;
  }
}

function setFilter(f) {
  COMPANY_FILTER = f;
  renderCompanyTable();
}

function filterCompanies() {
  COMPANY_SEARCH = document.getElementById('company-search').value.toLowerCase();
  renderCompanyTable();
}

let EXPANDED_COMPANY = null;  // ticker of currently expanded row
let COMPANY_DETAILS = {};    // cache: ticker -> detail data

async function toggleCompanyDetail(ticker) {
  if (EXPANDED_COMPANY === ticker) {
    EXPANDED_COMPANY = null;
    renderCompanyTable();
    return;
  }
  EXPANDED_COMPANY = ticker;
  // Show loading immediately
  renderCompanyTable();
  if (!COMPANY_DETAILS[ticker]) {
    try {
      const resp = await fetch('/api/status/companies/' + encodeURIComponent(ticker));
      if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${resp.statusText}`);
      const data = await resp.json();
      if (data.error) throw new Error(data.error);
      COMPANY_DETAILS[ticker] = data;
    } catch (e) {
      COMPANY_DETAILS[ticker] = {error: e.message};
    }
    renderCompanyTable();
  }
}

function renderDetailPanel(ticker) {
  const d = COMPANY_DETAILS[ticker];
  if (!d) return '<div class="detail-panel dp-loading">Loading details...</div>';
  if (d.error) return `<div class="detail-panel"><span style="color:#f87171">Error: ${d.error}</span></div>`;

  const tag = (cls, text) => text ? `<span class="dp-tag ${cls}">${text}</span>` : '';
  const dollarFmt = (v) => v != null ? '$' + v.toLocaleString(undefined, {maximumFractionDigits: 0}) : '';
  let h = '<div class="detail-panel">';

  // Company header
  h += `<div style="margin-bottom:12px"><strong style="color:#a5b4fc;font-size:15px">${d.ticker}</strong> <span style="color:#aaa">${d.company_name || ''}</span>`;
  if (d.sector) h += ` <span style="color:#555;font-size:12px">| ${d.sector}${d.industry ? ' / ' + d.industry : ''}</span>`;
  h += `<span style="color:#555;font-size:11px;margin-left:12px">${d.summary.financial_records} financials, ${d.summary.filings_count} filings, ${d.summary.evidence_count} evidence records</span></div>`;

  // Collect: financials
  h += '<div class="dp-section"><div class="dp-title" style="color:#34d399">Collect — Financial Data</div>';
  if (d.financials.length > 0) {
    // Deduplicate to latest per metric
    const byMetric = {};
    d.financials.forEach(f => { if (!byMetric[f.metric]) byMetric[f.metric] = f; });
    h += '<table class="dp-table"><tr><th>Metric</th><th>Value</th><th>Units</th><th>Source</th><th>Period</th></tr>';
    for (const [metric, f] of Object.entries(byMetric)) {
      const val = f.units === 'usd' ? dollarFmt(f.value) : fmt(f.value);
      h += `<tr><td>${metric}</td><td style="font-variant-numeric:tabular-nums">${val}</td><td>${f.units}</td><td>${f.source||''}</td><td>${f.period||'—'}</td></tr>`;
    }
    h += '</table>';
  } else {
    h += '<div class="dp-empty">No financial data collected</div>';
  }
  h += '</div>';

  // Collect: filings
  h += '<div class="dp-section"><div class="dp-title" style="color:#34d399">Collect — SEC Filings</div>';
  if (d.filings.length > 0) {
    h += '<table class="dp-table"><tr><th>Filing</th><th>Size</th></tr>';
    for (const f of d.filings) {
      h += `<tr><td>${f.filename}</td><td>${f.size_kb} KB</td></tr>`;
    }
    h += '</table>';
  } else {
    h += '<div class="dp-empty">No SEC filings on disk</div>';
  }
  h += '</div>';

  // Evidence grouped by type
  const evByType = {};
  (d.evidence || []).forEach(e => {
    (evByType[e.type] = evByType[e.type] || []).push(e);
  });

  const evTypeLabels = {
    filing_nlp: {label: 'Extract — Filing NLP', color: '#38bdf8'},
    product: {label: 'Extract — Products (GNews)', color: '#38bdf8'},
    cost_opportunity: {label: 'Value — Cost Opportunity', color: '#818cf8'},
    revenue_opportunity: {label: 'Value — Revenue Opportunity', color: '#818cf8'},
  };

  for (const [evType, items] of Object.entries(evByType)) {
    const meta = evTypeLabels[evType] || {label: evType, color: '#888'};
    h += `<div class="dp-section"><div class="dp-title" style="color:${meta.color}">${meta.label} (${items.length})</div>`;
    h += '<table class="dp-table"><tr><th>Target</th><th>Stage</th><th>Source</th><th>$ Est</th><th>Excerpt</th><th>Date</th></tr>';
    const shown = items.slice(0, 10);
    for (const e of shown) {
      h += `<tr>
        <td>${tag(e.target || '', e.target || '')}</td>
        <td>${tag(e.stage || '', e.stage || '')}</td>
        <td>${e.source || '—'}</td>
        <td style="font-variant-numeric:tabular-nums">${e.dollar_usd != null ? dollarFmt(e.dollar_usd) : '—'}</td>
        <td><div class="dp-excerpt">${e.excerpt || '—'}</div></td>
        <td style="white-space:nowrap">${e.source_date || '—'}</td>
      </tr>`;
    }
    h += '</table>';
    if (items.length > 10) h += `<div style="color:#555;font-size:11px;margin-top:4px">+ ${items.length - 10} more</div>`;
    h += '</div>';
  }

  if (Object.keys(evByType).length === 0) {
    h += '<div class="dp-section"><div class="dp-title" style="color:#38bdf8">Extract / Value</div><div class="dp-empty">No evidence records</div></div>';
  }

  // Score
  h += '<div class="dp-section"><div class="dp-title" style="color:#fbbf24">Score</div>';
  if (d.score) {
    const s = d.score;
    h += `<table class="dp-table"><tr><th>Metric</th><th>Value</th></tr>`;
    h += `<tr><td>Quadrant</td><td>${s.quadrant_label || s.quadrant || '—'}</td></tr>`;
    if (s.rank != null) h += `<tr><td>Rank</td><td>#${s.rank}</td></tr>`;
    h += `<tr><td>Opportunity</td><td>${s.opportunity?.toFixed(2) ?? '—'}</td></tr>`;
    h += `<tr><td>Realization</td><td>${s.realization?.toFixed(2) ?? '—'}</td></tr>`;
    if (s.cost_opp_score != null) h += `<tr><td>Cost Opp Score</td><td>${s.cost_opp_score.toFixed(3)}</td></tr>`;
    if (s.revenue_opp_score != null) h += `<tr><td>Revenue Opp Score</td><td>${s.revenue_opp_score.toFixed(3)}</td></tr>`;
    if (s.cost_capture_score != null) h += `<tr><td>Cost Capture Score</td><td>${s.cost_capture_score.toFixed(3)}</td></tr>`;
    if (s.revenue_capture_score != null) h += `<tr><td>Revenue Capture Score</td><td>${s.revenue_capture_score.toFixed(3)}</td></tr>`;
    h += `<tr><td colspan="2" style="font-weight:600; padding-top:8px">Sub-Scorer Breakdown</td></tr>`;
    if (s.filing_nlp_score != null) h += `<tr><td>Filing NLP</td><td>${s.filing_nlp_score.toFixed(3)}</td></tr>`;
    if (s.product_score != null) h += `<tr><td>Product/News</td><td>${s.product_score.toFixed(3)}</td></tr>`;
    if (s.github_score != null) h += `<tr><td>GitHub AI Activity</td><td>${s.github_score.toFixed(3)}</td></tr>`;
    if (s.analyst_score != null) h += `<tr><td>Analyst Consensus</td><td>${s.analyst_score.toFixed(3)}</td></tr>`;
    h += `<tr><td>Scored At</td><td>${s.scored_at || '—'}</td></tr>`;
    if (s.flags && s.flags.length) h += `<tr><td>Flags</td><td>${s.flags.join(', ')}</td></tr>`;
    h += '</table>';
  } else {
    h += '<div class="dp-empty">Not yet scored</div>';
  }
  h += '</div>';

  h += '</div>';
  return h;
}

function renderCompanyTable() {
  if (!COMPANIES) return;
  let filtered = COMPANIES;
  if (COMPANY_FILTER === 'incomplete') filtered = filtered.filter(c => !c.has_score);
  if (COMPANY_FILTER === 'complete') filtered = filtered.filter(c => c.has_score);
  if (COMPANY_FILTER === 'no_links') filtered = filtered.filter(c => !c.has_careers_url && !c.has_ir_url && !c.has_blog_url);
  if (COMPANY_FILTER === 'has_links') filtered = filtered.filter(c => c.has_careers_url || c.has_ir_url || c.has_blog_url);
  if (COMPANY_SEARCH) filtered = filtered.filter(c =>
    c.ticker.toLowerCase().includes(COMPANY_SEARCH) ||
    (c.company_name || '').toLowerCase().includes(COMPANY_SEARCH)
  );

  const icon = (v) => v ? '<span class="check">&#x2713;</span>' : '<span class="cross">&#x2717;</span>';
  let h = `<div class="drilldown-wrap">
    <h2>Company Pipeline Status</h2>
    <div class="drilldown-controls">
      <input class="drilldown-search" id="company-search" placeholder="Search ticker or name..." oninput="filterCompanies()" value="${COMPANY_SEARCH}">
      <button class="filter-btn ${COMPANY_FILTER==='all'?'active':''}" onclick="setFilter('all')">All (${COMPANIES.length})</button>
      <button class="filter-btn ${COMPANY_FILTER==='incomplete'?'active':''}" onclick="setFilter('incomplete')">Incomplete (${COMPANIES.filter(c=>!c.has_score).length})</button>
      <button class="filter-btn ${COMPANY_FILTER==='complete'?'active':''}" onclick="setFilter('complete')">Complete (${COMPANIES.filter(c=>c.has_score).length})</button>
      <button class="filter-btn ${COMPANY_FILTER==='no_links'?'active':''}" onclick="setFilter('no_links')">No Links (${COMPANIES.filter(c=>!c.has_careers_url&&!c.has_ir_url&&!c.has_blog_url).length})</button>
      <button class="filter-btn ${COMPANY_FILTER==='has_links'?'active':''}" onclick="setFilter('has_links')">Has Links (${COMPANIES.filter(c=>c.has_careers_url||c.has_ir_url||c.has_blog_url).length})</button>
      <button class="load-btn" onclick="loadCompanies()">Reload</button>
    </div>
    <div style="max-height:600px;overflow-y:auto;">
    <table class="company-table">
      <thead><tr><th>Ticker</th><th>Name</th><th class="stage-col">Collect</th><th class="stage-col">Extract</th><th class="stage-col">Value</th><th class="stage-col">Score</th><th class="stage-col" title="GitHub URL">GH</th><th class="stage-col" title="Careers URL">Car</th><th class="stage-col" title="Investor Relations URL">IR</th><th class="stage-col" title="Blog/Newsroom URL">Blog</th><th class="stage-col" title="News Cache">News</th><th class="stage-col" title="GitHub Cache">Git</th><th class="stage-col" title="Analyst Cache">Ana</th><th class="stage-col" title="Web Enrichment Cache">Web</th></tr></thead>
      <tbody>`;
  for (const c of filtered.slice(0, 200)) {
    const isExpanded = EXPANDED_COMPANY === c.ticker;
    h += `<tr class="company-row" onclick="toggleCompanyDetail('${c.ticker}')">
      <td style="font-weight:600;color:#a5b4fc">${isExpanded ? '&#x25BC;' : '&#x25B6;'} ${c.ticker}</td>
      <td>${c.company_name || '—'}</td>
      <td class="stage-cell">${icon(c.has_collect)}</td>
      <td class="stage-cell">${icon(c.has_extract)}</td>
      <td class="stage-cell">${icon(c.has_value)}</td>
      <td class="stage-cell">${icon(c.has_score)}</td>
      <td class="stage-cell">${icon(c.has_github_url)}</td>
      <td class="stage-cell">${icon(c.has_careers_url)}</td>
      <td class="stage-cell">${icon(c.has_ir_url)}</td>
      <td class="stage-cell">${icon(c.has_blog_url)}</td>
      <td class="stage-cell">${icon(c.has_news)}</td>
      <td class="stage-cell">${icon(c.has_github)}</td>
      <td class="stage-cell">${icon(c.has_analyst)}</td>
      <td class="stage-cell">${icon(c.has_web_enrichment)}</td>
    </tr>`;
    if (isExpanded) {
      h += `<tr class="detail-row"><td colspan="14">${renderDetailPanel(c.ticker)}</td></tr>`;
    }
  }
  if (filtered.length > 200) {
    h += `<tr><td colspan="16" style="color:#666;text-align:center;padding:12px">Showing 200 of ${filtered.length} companies. Use search to narrow.</td></tr>`;
  }
  if (filtered.length === 0) {
    h += `<tr><td colspan="16" style="color:#666;text-align:center;padding:12px">No companies match.</td></tr>`;
  }
  h += '</tbody></table></div></div>';
  document.getElementById('company-drilldown').innerHTML = h;
}

// ── Heatmap ──
function renderHeatmap(d) {
  const daily = d.daily || {};
  const sources = d.sources || [];

  const allDates = new Set();
  for (const src of SOURCE_ORDER) {
    if (daily[src]) Object.keys(daily[src]).forEach(d => allDates.add(d));
  }
  for (const [src, days] of Object.entries(daily)) {
    if (!SOURCE_ORDER.includes(src)) Object.keys(days).forEach(d => allDates.add(d));
  }
  const sortedDates = [...allDates].sort().reverse();
  const extraSources = Object.keys(daily).filter(s => !SOURCE_ORDER.includes(s));
  const allSources = [...SOURCE_ORDER, ...extraSources];

  const maxPerSource = {};
  for (const src of allSources) {
    let mx = 0;
    if (daily[src]) {
      for (const day of Object.values(daily[src])) {
        const v = MODE === 'companies' ? (day.companies || 0) : (day.records || 0);
        if (v > mx) mx = v;
      }
    }
    maxPerSource[src] = mx;
  }

  const PHASE_COLORS = {'1-collect':'#34d399','2-extract':'#38bdf8','3-value':'#818cf8','4-score':'#fbbf24'};
  const PHASE_LABELS = {'1-collect':'COLLECT','2-extract':'EXTRACT','3-value':'VALUE','4-score':'SCORE'};

  let h = '<div class="heatmap-wrap">';
  h += '<h2>Pipeline Coverage by Day</h2>';
  h += `<div class="help">Showing: ${MODE === 'companies' ? 'distinct companies' : 'total records'} per source per day. Hover cells for details.</div>`;
  h += '<table class="heatmap"><thead><tr><th class="date-col">Date</th>';

  let lastPhase = '';
  for (const src of allSources) {
    const meta = SOURCE_META[src] || {};
    const phase = meta.phase || '';
    const short = meta.short || src;
    const catColor = PHASE_COLORS[phase] || '#888';
    const catLabel = PHASE_LABELS[phase] || '';
    h += `<th style="cursor:help">`;
    const phaseNum = phase ? phase.charAt(0) : '';
    h += `<span class="phase-header" style="color:${catColor}">${phase !== lastPhase ? phaseNum + '. ' + catLabel : ''}</span><br>`;
    h += `<span style="border-bottom:1px dotted ${catColor}">${short}</span></th>`;
    lastPhase = phase;
  }
  h += '</tr></thead><tbody>';

  for (const dt of sortedDates) {
    const isToday = dt === new Date().toISOString().slice(0, 10);
    h += '<tr>';
    h += `<td class="date-cell">${isToday ? 'Today' : dt}</td>`;
    for (const src of allSources) {
      const day = daily[src]?.[dt];
      const companies = day?.companies || 0;
      const records = day?.records || 0;
      const val = MODE === 'companies' ? companies : records;
      if (val === 0) {
        h += '<td class="empty heat-0">&mdash;</td>';
      } else {
        h += `<td class="${heatClass(val, maxPerSource[src])}">`;
        h += `<span class="count">${fmt(val)}</span>`;
        h += `<div class="tip"><strong>${src}</strong><br>${dt}${isToday ? ' (today)' : ''}<br>Companies: ${fmt(companies)}<br>Records: ${fmt(records)}</div>`;
        h += '</td>';
      }
    }
    h += '</tr>';
  }

  // Totals row
  const srcByName = {};
  sources.forEach(s => srcByName[s.name] = s);
  h += '<tr><td class="date-cell col-total">Total (unique)</td>';
  for (const src of allSources) {
    const apiSrc = srcByName[src];
    let total = 0;
    if (daily[src]) {
      for (const day of Object.values(daily[src])) {
        total += MODE === 'companies' ? (day.companies || 0) : (day.records || 0);
      }
    }
    const val = apiSrc ? (MODE === 'companies' ? apiSrc.companies : apiSrc.records) : total;
    h += `<td class="col-total">${fmt(val)}</td>`;
  }
  h += '</tr></tbody></table></div>';
  document.getElementById('heatmap').innerHTML = h;
}

// ── Pipeline Runs + Cost ──
function renderExtras(d) {
  let ex = '';
  const s = d.scoring;

  // Task summary cards
  if (s.task_summary && s.task_summary.length > 0) {
    ex += '<div class="card"><h2>Pipeline Runs</h2>';
    ex += '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:16px">';
    const taskColors = {collect:'#34d399',extract:'#38bdf8',value:'#818cf8',score:'#fbbf24'};
    const taskLabels = {collect:'Collect',extract:'Extract',value:'Value',score:'Score'};
    const subtaskLabels = {links:'Links',yahoo_fundamentals:'Yahoo Fundamentals',sec_filings:'SEC Filings',news:'News',github:'GitHub',analysts:'Analysts',web_enrichment:'Web Enrichment',all:'All'};
    for (const st of s.task_summary) {
      const color = taskColors[st.task] || '#888';
      const statusBadge = st.currently_running > 0
        ? '<span class="badge running">running</span>'
        : st.last_status === 'completed'
          ? '<span class="badge completed">ok</span>'
          : st.last_status === 'failed'
            ? '<span class="badge failed">failed</span>'
            : '<span style="color:#666">—</span>';
      ex += `<div style="background:#1a1a2e;border-radius:8px;padding:12px;border-left:3px solid ${color}">
        <div style="font-weight:600;color:${color}">${taskLabels[st.task]||st.task}</div>
        <div style="font-size:12px;color:#aaa;margin-top:4px">${st.total_runs} runs ${statusBadge}</div>
        <div style="font-size:11px;color:#666;margin-top:4px">${st.last_completed ? 'Last: '+new Date(st.last_completed).toLocaleString() : 'Never completed'}</div>
      </div>`;
    }
    ex += '</div>';

    // Detailed runs table
    if (s.recent_runs && s.recent_runs.length > 0) {
      ex += '<table class="detail"><tr><th>Run</th><th>Task</th><th>Subtask</th><th>Type</th><th>Status</th><th>OK</th><th>Fail</th><th>Started</th></tr>';
      for (const r of s.recent_runs) {
        const rst = r.status || 'unknown';
        const color = taskColors[r.task] || '#888';
        ex += `<tr><td style="font-family:monospace;font-size:11px">${(r.run_id||'').slice(0,8)}</td>
          <td><span style="color:${color}">${taskLabels[r.task]||r.task||''}</span></td>
          <td>${subtaskLabels[r.subtask]||r.subtask||''}</td>
          <td>${r.run_type||''}</td>
          <td><span class="badge ${rst}">${rst}</span></td>
          <td>${fmt(r.tickers_succeeded)}</td><td>${fmt(r.tickers_failed)}</td>
          <td style="font-size:12px">${r.started_at ? new Date(r.started_at).toLocaleString() : '—'}</td></tr>`;
      }
      ex += '</table>';
    }
    ex += '</div>';
  }

  if (d.cost) {
    const cost = d.cost;
    ex += `<div class="card"><h2>Cost Detail</h2>`;
    ex += `<p style="color:#aaa;font-size:13px">Duration: ${(cost.elapsed_seconds/60).toFixed(1)} min | Total: <strong style="color:#fbbf24">$${cost.total_estimated_cost_usd?.toFixed(4)||'0'}</strong></p>`;
    if (cost.api_calls) {
      ex += '<table class="detail"><tr><th>API</th><th>Calls</th><th>MB</th><th>Errors</th><th>Est Cost</th></tr>';
      for (const [api, calls] of Object.entries(cost.api_calls)) {
        const mb = cost.mb_downloaded?.[api] || 0;
        const errs = cost.api_errors?.[api] || 0;
        const c2 = cost.estimated_cost_usd?.[api] || 0;
        ex += `<tr><td>${api}</td><td>${fmt(calls)}</td><td>${mb.toFixed(2)}</td><td>${errs}</td><td>$${c2.toFixed(4)}</td></tr>`;
      }
      ex += '</table>';
    }
    ex += '</div>';
  }
  document.getElementById('extras').innerHTML = ex;
}

// ── Main render ──
function render(d) {
  DATA = d;
  renderPipeline(d);
  renderOverall(d);
  renderHealth(d);
  renderStageDetails(d);
  if (COMPANIES) renderCompanyTable();
  renderHeatmap(d);
  renderExtras(d);
  document.getElementById('updated').textContent = 'Updated ' + new Date().toLocaleTimeString();
}

async function loadStatus() {
  try {
    const [resp, extResp] = await Promise.all([
      fetch('/api/status'),
      EXTRACTORS ? Promise.resolve(null) : fetch('/api/status/extractors'),
    ]);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${resp.statusText}`);
    const data = await resp.json();
    if (!data.pipeline) throw new Error('API response missing "pipeline" key: ' + JSON.stringify(Object.keys(data)));
    if (!data.sources) throw new Error('API response missing "sources" key');
    if (extResp && extResp.ok) {
      const extData = await extResp.json();
      EXTRACTORS = extData.extractors || [];
    }
    render(data);
  } catch (e) {
    document.getElementById('pipeline-funnel').innerHTML = '<p style="color:#f87171;padding:20px">Error: ' + e.message + '</p>';
  }
}

document.getElementById('auto-toggle').addEventListener('change', function() {
  if (this.checked) timer = setInterval(loadStatus, 10000);
  else clearInterval(timer);
});

loadStatus();
timer = setInterval(loadStatus, 10000);
</script>
</body>
</html>"""
    return Response(content=html, media_type="text/html")


# ── Landing Page & Static Assets ─────────────────────────────────────────


@get("/")
async def landing_page() -> Any:
    from litestar.response import Response
    html_path = LANDING_DIR / "index.html"
    return Response(content=html_path.read_text(), media_type="text/html")


@get("/whitepaper")
async def whitepaper_page() -> Any:
    from litestar.response import Response
    html_path = LANDING_DIR / "whitepaper.html"
    if html_path.exists():
        return Response(content=html_path.read_text(), media_type="text/html")
    return Response(content="<html><body><p>Coming soon.</p></body></html>", media_type="text/html")


@get("/methodology")
async def methodology_page() -> Any:
    from litestar.response import Response
    html_path = LANDING_DIR / "methodology.html"
    if html_path.exists():
        return Response(content=html_path.read_text(), media_type="text/html")
    return Response(content="<html><body><p>Coming soon.</p></body></html>", media_type="text/html")


@get("/static/style.css")
async def serve_css() -> File:
    return File(path=ASSETS_DIR / "style.css", media_type="text/css")


@get("/static/data/{filename:str}")
async def serve_data(filename: str) -> Any:
    from litestar.response import Response
    filepath = PROCESSED_DIR / filename
    if not filepath.exists():
        return Response(
            content='{"error": "not found"}',
            media_type="application/json",
            status_code=HTTP_404_NOT_FOUND,
        )
    media = "application/json" if filename.endswith(".json") else "text/csv"
    return File(path=filepath, media_type=media)


# ── Login ─────────────────────────────────────────────────────────────────


@post("/api/login")
async def login(request: Request) -> dict:
    body = await request.json()
    email = body.get("email", "").strip().lower()

    if not email:
        return {"error": "Email is required"}

    subscriber = get_subscriber_by_email(email)
    if not subscriber or subscriber.status != "active":
        return {"error": "No active subscription found for that email."}

    dashboard_url = f"{BASE_URL}/dashboard?token={subscriber.access_token}"

    if RESEND_API_KEY:
        try:
            resend.Emails.send({
                "from": f"Winona Quantitative Research <{FROM_EMAIL}>",
                "to": [email],
                "subject": "Your AI Opportunity Index Login Link",
                "html": (
                    "<div style='font-family:sans-serif;max-width:600px;margin:0 auto;padding:20px'>"
                    "<h1 style='color:#1a1a2e'>AI Opportunity Index</h1>"
                    "<p>Click the link below to access your dashboard:</p>"
                    f"<p><a href='{dashboard_url}' style='display:inline-block;background:#6366f1;"
                    "color:white;padding:12px 24px;border-radius:8px;text-decoration:none;"
                    "font-weight:600'>Open Dashboard</a></p>"
                    "<p style='color:#999;font-size:12px'>If you didn't request this, you can ignore this email.</p>"
                    "</div>"
                ),
            })
        except Exception as e:
            logger.error("Failed to send login email to %s: %s", email, e)
            return {"error": "Failed to send email. Please try again."}

    return {"ok": True}


# ── Stripe Checkout ───────────────────────────────────────────────────────


@post("/api/create-checkout-session")
async def create_checkout_session() -> dict:
    if not STRIPE_SECRET_KEY or not STRIPE_PRICE_ID:
        return {"error": "Stripe not configured", "url": None}

    try:
        session = stripe.checkout.Session.create(
            mode="subscription",
            payment_method_types=["card"],
            line_items=[{"price": STRIPE_PRICE_ID, "quantity": 1}],
            success_url=f"{BASE_URL}/api/checkout-success?session_id={{CHECKOUT_SESSION_ID}}",
            cancel_url=f"{BASE_URL}/#subscribe",
        )
        return {"url": session.url}
    except stripe.error.StripeError as e:
        logger.error("Stripe checkout error: %s", e)
        return {"error": str(e)}


@get("/api/checkout-success")
async def checkout_success(session_id: str) -> Redirect:
    if not STRIPE_SECRET_KEY:
        return Redirect(path="/")

    try:
        session = stripe.checkout.Session.retrieve(session_id, expand=["subscription"])
        email = session.customer_details.email
        customer_id = session.customer
        subscription_id = session.subscription.id if session.subscription else None

        token = create_subscriber(
            email=email,
            stripe_customer_id=customer_id,
            stripe_subscription_id=subscription_id,
        )

        dashboard_url = f"{BASE_URL}/dashboard?token={token}"
        send_welcome_email(email, dashboard_url)

        return Redirect(path=f"/dashboard?token={token}")
    except Exception as e:
        logger.error("Checkout success error: %s", e)
        return Redirect(path="/")


# ── Stripe Webhook ────────────────────────────────────────────────────────


@post("/api/webhook")
async def stripe_webhook(request: Request) -> dict:
    if not STRIPE_WEBHOOK_SECRET:
        return {"error": "Webhook not configured"}

    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
    except (ValueError, stripe.error.SignatureVerificationError) as e:
        logger.error("Webhook signature verification failed: %s", e)
        return {"error": "Invalid signature"}

    event_type = event["type"]
    data = event["data"]["object"]

    if event_type == "checkout.session.completed":
        email = data.get("customer_details", {}).get("email")
        customer_id = data.get("customer")
        subscription_id = data.get("subscription")
        if email:
            create_subscriber(
                email=email,
                stripe_customer_id=customer_id,
                stripe_subscription_id=subscription_id,
            )
            logger.info("Subscriber created via webhook: %s", email)

    elif event_type == "customer.subscription.deleted":
        sub_id = data.get("id")
        if sub_id:
            update_subscriber_status(sub_id, "canceled")
            logger.info("Subscription canceled: %s", sub_id)

    elif event_type == "invoice.payment_failed":
        sub_id = data.get("subscription")
        if sub_id:
            update_subscriber_status(sub_id, "past_due")
            logger.info("Payment failed for subscription: %s", sub_id)

    return {"status": "ok"}


# ── Stripe Customer Portal ────────────────────────────────────────────────


@get("/api/customer-portal")
async def customer_portal(token: str) -> Any:
    from litestar.response import Response
    if not STRIPE_SECRET_KEY:
        return Redirect(path="/")

    subscriber = get_subscriber_by_token(token)
    if not subscriber or not subscriber.stripe_customer_id:
        return Response(
            content=(
                "<html><body style='font-family:sans-serif;padding:40px;text-align:center'>"
                "<h2>Access Required</h2>"
                "<p>No subscription found for this account.</p>"
                "</body></html>"
            ),
            media_type="text/html",
            status_code=HTTP_403_FORBIDDEN,
        )

    try:
        portal_session = stripe.billing_portal.Session.create(
            customer=subscriber.stripe_customer_id,
            return_url=f"{BASE_URL}/dashboard?token={token}",
        )
        return Redirect(path=portal_session.url)
    except stripe.error.StripeError as e:
        logger.error("Customer portal error: %s", e)
        return Response(
            content=f"<html><body><p>Error: {e}</p></body></html>",
            media_type="text/html",
            status_code=HTTP_500_INTERNAL_SERVER_ERROR,
        )


# ── Dashboard ─────────────────────────────────────────────────────────────


@get("/dashboard")
async def dashboard(request: Request) -> Any:
    from litestar.response import Response
    token = request.query_params.get("token")

    denied_html = (
        "<html><body style='font-family:sans-serif;padding:40px;text-align:center'>"
        "<h2>Access Required</h2>"
        "<p>You need a valid subscription to access the dashboard.</p>"
        f"<a href='{BASE_URL}/#subscribe'>Subscribe now</a>"
        "</body></html>"
    )

    if not token:
        return Response(content=denied_html, media_type="text/html", status_code=HTTP_403_FORBIDDEN)

    subscriber = get_subscriber_by_token(token)
    if not subscriber or subscriber.status != "active":
        return Response(
            content=(
                "<html><body style='font-family:sans-serif;padding:40px;text-align:center'>"
                "<h2>Invalid or Expired Access</h2>"
                "<p>Your subscription may have expired or the link is invalid.</p>"
                f"<a href='{BASE_URL}/#subscribe'>Subscribe now</a>"
                "</body></html>"
            ),
            media_type="text/html",
            status_code=HTTP_403_FORBIDDEN,
        )

    # Serve the static dashboard HTML
    dashboard_path = LANDING_DIR / "dashboard.html"
    if dashboard_path.exists():
        html = dashboard_path.read_text().replace("{{TOKEN}}", token)
        return Response(content=html, media_type="text/html")

    return Response(
        content="<html><body><p>Dashboard coming soon.</p></body></html>",
        media_type="text/html",
    )


@get("/company/{ticker:str}")
async def company_profile(request: Request, ticker: str) -> Any:
    """Full-page company profile — opens in new tab from dashboard.

    Child share-class tickers (e.g. GOOG) render the same page template;
    the API resolves to canonical data and includes alias metadata so the
    frontend can show a secondary-listing banner.
    """
    from litestar.response import Response
    token = request.query_params.get("token", "")
    # Auth check (same as dashboard)
    subscriber = get_subscriber_by_token(token) if token else None
    if not subscriber or subscriber.status != "active":
        return Response(
            content="<html><body style='font-family:sans-serif;padding:40px;text-align:center'>"
            "<h2>Access Required</h2><p>You need a valid subscription.</p></body></html>",
            media_type="text/html", status_code=HTTP_403_FORBIDDEN,
        )

    profile_path = LANDING_DIR / "company_profile.html"
    if profile_path.exists():
        html = profile_path.read_text().replace("{{TOKEN}}", token).replace("{{TICKER}}", ticker.upper())
        return Response(content=html, media_type="text/html")

    return Response(content="<html><body><p>Company profile not found.</p></body></html>", media_type="text/html")


# ── API: Companies ────────────────────────────────────────────────────────


@get("/api/companies")
async def api_companies(
    request: Request,
) -> dict:
    """Paginated company list with latest scores."""
    sector = request.query_params.get("sector")
    quadrant = request.query_params.get("quadrant")
    industry = request.query_params.get("industry")
    sort_by = request.query_params.get("sort_by")
    limit = int(request.query_params.get("limit", "100"))
    offset = int(request.query_params.get("offset", "0"))

    try:
        rows = get_latest_scores(
            sector=sector, quadrant=quadrant, industry=industry,
            sort_by=sort_by, limit=limit, offset=offset,
        )
        return {"companies": rows, "count": len(rows)}
    except Exception as e:
        logger.error("Error fetching companies: %s", e)
        return {"companies": [], "count": 0, "error": str(e)}


@get("/api/companies/{ticker:str}")
async def api_company_detail(ticker: str) -> dict:
    """Company detail with scores and evidence."""
    import json as _json
    from ai_opportunity_index.config import RAW_DIR

    detail = get_company_detail(ticker)
    if not detail:
        return {"error": "Company not found"}

    # Attach cached enrichment data from disk (canonical ticker/slug + children)
    primary_ident = detail["ticker"] or detail.get("slug")
    all_idents = [primary_ident] if primary_ident else []
    all_idents.extend(detail.get("child_tickers") or [])
    enrichment = {}
    for source in ("github", "analysts", "web_enrichment"):
        for t in all_idents:
            if not t:
                continue
            cache_path = RAW_DIR / source / f"{t.upper()}.json"
            if cache_path.exists():
                try:
                    data = _json.loads(cache_path.read_text())
                    if source not in enrichment:
                        enrichment[source] = data
                    elif source == "web_enrichment":
                        # Merge child web enrichment sections
                        for key in ("careers", "investor_relations", "blog"):
                            if data.get(key) and not enrichment[source].get(key):
                                enrichment[source][key] = data[key]
                except Exception:
                    pass
        if source not in enrichment:
            enrichment[source] = None
    detail["enrichment"] = enrichment

    # Add AI Index rank across the scored universe (use canonical ticker)
    rank_info = get_ai_index_rank(detail["ticker"] or detail.get("slug", ""))
    detail["ai_index_rank"] = rank_info["rank"]
    detail["ai_index_total"] = rank_info["total"]

    return detail


@get("/api/companies/{ticker:str}/peers")
async def api_company_peers(ticker: str) -> dict:
    """Industry peer comparison for a company."""
    peers = get_industry_peers(ticker)
    return {"ticker": ticker, "peers": peers, "count": len(peers)}


@get("/api/companies/{ticker:str}/valuations")
async def api_company_valuations(ticker: str) -> dict:
    """Structured valuation data for the evidence viewer."""
    detail = get_company_valuation_detail(ticker)
    if not detail:
        return {"error": "No valuation data found", "ticker": ticker.upper()}
    return detail



# Pipeline status, run-stage, cancel-run, and runs endpoints are now in
# web/pipeline_controller.py (PipelineAPIController).



def _company_record_from_row(row) -> CompanyRecord:
    """Build a CompanyRecord from a DB row."""
    return CompanyRecord(
        id=row.id,
        ticker=row.ticker,
        slug=row.slug,
        company_name=row.company_name,
        exchange=row.exchange,
        sector=row.sector,
        industry=row.industry,
        github_url=row.github_url,
        careers_url=row.careers_url,
        ir_url=row.ir_url,
        blog_url=row.blog_url,
        is_active=row.is_active,
        updated_at=row.updated_at,
    )


@get("/api/companies/{ticker:str}/links")
async def api_company_links(ticker: str) -> dict:
    """Get company record with links."""
    from ai_opportunity_index.storage.models import CompanyModel

    with get_session() as session:
        row = session.query(CompanyModel).filter(CompanyModel.ticker == ticker.upper()).first()
        if not row:
            row = session.query(CompanyModel).filter(CompanyModel.slug == ticker.upper()).first()
        if not row:
            return {"error": "Company not found"}
        record = _company_record_from_row(row)

    return record.model_dump()


@put("/api/companies/{ticker:str}/links")
async def api_update_company_links(ticker: str, data: CompanyUpdate) -> dict:
    """Update company fields. Returns the updated company record."""
    from sqlalchemy import text as sa_text
    from ai_opportunity_index.storage.models import CompanyModel

    company = get_company_by_ticker(ticker)
    if not company:
        return {"error": "Company not found"}

    # Only apply fields that were explicitly provided (not just defaulting to None)
    provided = data.model_dump(exclude_unset=True)
    if not provided:
        return {"error": "No fields provided"}

    # Only allow safe fields to be updated
    allowed = {"company_name", "ticker", "exchange", "sector", "industry",
               "github_url", "careers_url", "ir_url", "blog_url"}
    updates = {}
    for k, v in provided.items():
        if k in allowed:
            updates[k] = v if v else None

    if not updates:
        return {"error": "No updatable fields provided"}

    with get_session() as session:
        set_clauses = ", ".join(f"{k} = :{k}" for k in updates)
        updates["cid"] = company.id
        session.execute(
            sa_text(f"UPDATE companies SET {set_clauses}, updated_at = now() WHERE id = :cid"),
            updates,
        )
        session.commit()

        # Return the updated record
        row = session.get(CompanyModel, company.id)
        record = _company_record_from_row(row)

    return record.model_dump()


@post("/api/companies/{ticker:str}/refresh")
async def api_request_refresh(ticker: str, request: Request) -> dict:
    """Request a data refresh for a company (auth required)."""
    token = request.query_params.get("token") or request.headers.get("authorization", "").replace("Bearer ", "")

    if not token:
        return {"error": "Authentication required"}

    subscriber = get_subscriber_by_token(token)
    if not subscriber or subscriber.status != "active":
        return {"error": "Active subscription required"}

    company = get_company_by_ticker(ticker)
    if not company:
        return {"error": "Company not found"}

    refresh_req = create_refresh_request(
        RefreshRequest(
            subscriber_id=subscriber.id,
            company_id=company.id,
        )
    )

    return {
        "ok": True,
        "request_id": refresh_req.id,
        "status": refresh_req.status,
        "message": f"Refresh requested for {ticker}. You will be notified when complete.",
    }


# ── API: Portfolios ──────────────────────────────────────────────────────


@get("/api/portfolios/{variant:str}")
async def api_portfolio(variant: str) -> Any:
    """Portfolio holdings for a given variant."""
    from litestar.response import Response
    import json

    holdings_path = PROCESSED_DIR / "top_holdings.json"
    if not holdings_path.exists():
        return {"error": "Holdings data not available"}

    with open(holdings_path) as f:
        all_holdings = json.load(f)

    if variant not in all_holdings:
        return {"error": f"Unknown variant: {variant}"}

    return {"variant": variant, "holdings": all_holdings[variant]}


# ── Application Lifecycle ─────────────────────────────────────────────────


async def on_startup() -> None:
    init_db()
    if STRIPE_SECRET_KEY:
        stripe.api_key = STRIPE_SECRET_KEY
    if RESEND_API_KEY:
        resend.api_key = RESEND_API_KEY
    if ADMIN_EMAIL:
        token = create_subscriber(email=ADMIN_EMAIL, stripe_customer_id=None, stripe_subscription_id=None)
        logger.info("Admin account seeded: %s (token: %s)", ADMIN_EMAIL, token)


app = Litestar(
    route_handlers=[
        landing_page,
        whitepaper_page,
        methodology_page,
        serve_css,
        serve_data,
        login,
        create_checkout_session,
        checkout_success,
        stripe_webhook,
        customer_portal,
        dashboard,
        api_status,
        api_status_extractors,
        api_status_companies,
        api_status_company_detail,
        status_page,
        company_profile,
        api_companies,
        api_company_detail,
        api_company_peers,
        api_company_valuations,
        PipelineAPIController,
        api_company_links,
        api_update_company_links,
        api_request_refresh,
        api_portfolio,
    ],
    on_startup=[on_startup],
)
