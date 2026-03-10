"""Litestar Controller grouping all pipeline-related API endpoints.

Extracted from web/app.py to centralise pipeline orchestration and
use the canonical DAG from ai_opportunity_index.pipeline.
"""

from __future__ import annotations

import asyncio
import json
import logging
import subprocess
import sys
import uuid
from pathlib import Path

from litestar import Controller, Request, get, post

from ai_opportunity_index.domains import RunStatus
from ai_opportunity_index.pipeline import PipelineController, PipelineRequest, TriggerSource
from ai_opportunity_index.pipeline.dag import DAG, topological_layers, resolve_stages
from ai_opportunity_index.storage.db import get_company_by_ticker, get_session

logger = logging.getLogger(__name__)

_active_procs: dict[str, subprocess.Popen] = {}


class PipelineAPIController(Controller):
    path = "/api"

    @get("/pipeline/stages")
    async def stages(self) -> dict:
        """Return canonical stage list + DAG for the web UI."""
        layers = topological_layers(DAG)
        ordered = [s for layer in layers for s in layer]
        return {
            "stages": ordered,
            "dependencies": {k: sorted(v) for k, v in DAG.items()},
        }

    @get("/companies/{ticker:str}/pipeline-status")
    async def status(self, ticker: str) -> dict:
        """Pipeline stage completion matrix for a company."""
        from sqlalchemy import text as sa_text
        from ai_opportunity_index.config import RAW_DIR

        company = get_company_by_ticker(ticker)
        if not company:
            return {"error": "Company not found"}

        cid = company.id
        stages = {}

        with get_session() as session:
            # 1. discover_links -- check if company has discovered URLs
            r = session.execute(
                sa_text("SELECT github_url, careers_url, ir_url, blog_url FROM companies WHERE id = :cid"),
                {"cid": cid},
            )
            link_row = r.fetchone()
            link_detail = {
                "github_url": bool(link_row[0]) if link_row else False,
                "careers_url": bool(link_row[1]) if link_row else False,
                "ir_url": bool(link_row[2]) if link_row else False,
                "blog_url": bool(link_row[3]) if link_row else False,
            }
            has_links = any(link_detail.values())
            stages["discover_links"] = {
                "label": "Discover Links",
                "done": has_links,
                "detail": link_detail,
            }

            # 2-5. collect_* stages -- check evidence by type + cache files
            collect_map = {
                "collect_news": {"label": "Collect News", "types": ["product"], "cache_dir": "news"},
                "collect_github": {"label": "Collect GitHub", "types": ["github"], "cache_dir": "github"},
                "collect_analysts": {"label": "Collect Analysts", "types": ["analyst"], "cache_dir": "analysts"},
                "collect_web_enrichment": {"label": "Collect Web", "types": ["web_enrichment"], "cache_dir": "web_enrichment"},
            }
            for stage_key, cfg in collect_map.items():
                r = session.execute(
                    sa_text("""
                        SELECT count(*), max(observed_at)
                        FROM evidence
                        WHERE company_id = :cid AND evidence_type = ANY(:types)
                    """),
                    {"cid": cid, "types": cfg["types"]},
                )
                row = r.fetchone()
                ev_count = row[0] or 0

                # Also check if cache file exists (stage ran but may have found nothing)
                cache_path = RAW_DIR / cfg["cache_dir"] / f"{ticker.upper()}.json"
                cache_exists = cache_path.exists()
                cache_collected_at = None
                cache_summary = None
                cache_stale = False
                if cache_exists and ev_count == 0:
                    try:
                        cache_data = json.loads(cache_path.read_text())
                        cache_collected_at = cache_data.get("collected_at")
                        if stage_key == "collect_github":
                            db_github = link_row[0] if link_row else None
                            org = cache_data.get("org_name")
                            if db_github and not org:
                                cache_summary = "GitHub URL set but not yet scraped"
                                cache_stale = True
                            elif not org:
                                cache_summary = "No public GitHub org found"
                            else:
                                cache_summary = f"Org: {org}, {cache_data.get('total_repos', 0)} repos"
                        elif stage_key == "collect_news":
                            articles = cache_data.get("article_count", 0)
                            cache_summary = "No AI-related news found" if articles == 0 else f"{articles} articles, no scorable evidence"
                        elif stage_key == "collect_analysts":
                            rec = cache_data.get("recommendation_key")
                            cache_summary = "No analyst coverage found" if not rec else f"Analysts found ({rec}) but no evidence rows"
                        elif stage_key == "collect_web_enrichment":
                            db_links = {
                                "careers": bool(link_row[1]) if link_row else False,
                                "investor_relations": bool(link_row[2]) if link_row else False,
                                "blog": bool(link_row[3]) if link_row else False,
                            }
                            parts = []
                            for page_key in ("careers", "investor_relations", "blog"):
                                page = cache_data.get(page_key)
                                has_db_link = db_links.get(page_key, False)
                                if page and isinstance(page, dict):
                                    items = len(page.get("evidence_items", []))
                                    parts.append(f"{page_key}: {items} items")
                                elif has_db_link:
                                    parts.append(f"{page_key}: link set, needs re-run")
                                    cache_stale = True
                                else:
                                    parts.append(f"{page_key}: no link")
                            cache_summary = "; ".join(parts) if parts else None
                    except Exception:
                        pass

                done = ev_count > 0 or (cache_exists and ev_count == 0)
                stages[stage_key] = {
                    "label": cfg["label"],
                    "done": done,
                    "count": ev_count,
                    "last_run": row[1].isoformat() if row[1] else cache_collected_at,
                    "checked_empty": cache_exists and ev_count == 0,
                    "stale": cache_stale,
                    "summary": cache_summary,
                }

            # 6. extract_filings -- check filing_nlp evidence
            r = session.execute(
                sa_text("""
                    SELECT count(*), max(observed_at)
                    FROM evidence
                    WHERE company_id = :cid AND evidence_type = 'filing_nlp'
                """),
                {"cid": cid},
            )
            row = r.fetchone()
            stages["extract_filings"] = {
                "label": "Extract Filings",
                "done": (row[0] or 0) > 0,
                "count": row[0] or 0,
                "last_run": row[1].isoformat() if row[1] else None,
            }

            # 7. extract_news -- check extract_news via evidence_groups
            r = session.execute(
                sa_text("""
                    SELECT count(*), max(created_at)
                    FROM evidence_groups
                    WHERE company_id = :cid
                """),
                {"cid": cid},
            )
            row = r.fetchone()
            stages["extract_news"] = {
                "label": "Extract News",
                "done": (row[0] or 0) > 0,
                "count": row[0] or 0,
                "last_run": row[1].isoformat() if row[1] else None,
            }

            # 7b. extract_unified -- same check as extract_news (groups)
            stages["extract_unified"] = {
                "label": "Extract & Group",
                "done": (row[0] or 0) > 0,
                "count": row[0] or 0,
                "last_run": row[1].isoformat() if row[1] else None,
            }

            # 8. value_evidence -- check valuations via groups
            r = session.execute(
                sa_text("""
                    SELECT count(*), max(v.created_at)
                    FROM valuations v
                    JOIN evidence_groups eg ON v.group_id = eg.id
                    WHERE eg.company_id = :cid AND v.stage = 'final'
                """),
                {"cid": cid},
            )
            row = r.fetchone()
            stages["value_evidence"] = {
                "label": "Value Evidence",
                "done": (row[0] or 0) > 0,
                "count": row[0] or 0,
                "last_run": row[1].isoformat() if row[1] else None,
            }

            # 9. score -- check company_scores
            r = session.execute(
                sa_text("""
                    SELECT count(*), max(scored_at)
                    FROM company_scores
                    WHERE company_id = :cid
                """),
                {"cid": cid},
            )
            row = r.fetchone()
            score_count = row[0] or 0

            ticker_ident = company.ticker or company.slug or ""
            score_last_run = row[1].isoformat() if row[1] else None
            score_checked_empty = False
            if score_count == 0 and ticker_ident:
                pr = session.execute(
                    sa_text("""
                        SELECT completed_at FROM pipeline_runs
                        WHERE :ticker = ANY(tickers_requested)
                          AND subtask LIKE '%score%'
                          AND status IN ('completed', 'RunStatus.COMPLETED')
                        ORDER BY completed_at DESC LIMIT 1
                    """),
                    {"ticker": ticker_ident},
                ).fetchone()
                if pr:
                    score_checked_empty = True
                    score_last_run = pr[0].isoformat() if pr[0] else score_last_run

            stages["score"] = {
                "label": "Score",
                "done": score_count > 0 or score_checked_empty,
                "checked_empty": score_checked_empty,
                "count": score_count,
                "last_run": score_last_run,
                "summary": "Ran but no scorable evidence" if score_checked_empty else None,
            }

            # 10. Count evidence groups with final valuations created after the latest score
            r = session.execute(
                sa_text("""
                    WITH latest_score AS (
                        SELECT max(scored_at) AS scored_at
                        FROM company_scores WHERE company_id = :cid
                    )
                    SELECT count(DISTINCT eg.id)
                    FROM evidence_groups eg
                    JOIN valuations v ON v.group_id = eg.id
                    CROSS JOIN latest_score ls
                    WHERE eg.company_id = :cid
                      AND v.stage = 'final'
                      AND v.created_at > COALESCE(ls.scored_at, '1970-01-01'::timestamp)
                """),
                {"cid": cid},
            )
            unscored_row = r.fetchone()
            stages["score"]["unscored_evidence"] = unscored_row[0] or 0

            # Cross-reference pipeline_runs to get the actual last-run time per stage
            run_rows = session.execute(
                sa_text("""
                    SELECT subtask, max(completed_at) AS last_completed
                    FROM pipeline_runs
                    WHERE :ticker = ANY(tickers_requested)
                      AND status IN ('completed', 'RunStatus.COMPLETED')
                    GROUP BY subtask
                """),
                {"ticker": ticker.upper()},
            ).fetchall()

            for run_row in run_rows:
                subtask, last_completed = run_row
                if not subtask or not last_completed:
                    continue
                # subtask is e.g. "collect_news+discover_links" or "all"
                stage_names = subtask.split("+") if subtask != "all" else list(stages.keys())
                completed_iso = last_completed.isoformat()
                for sname in stage_names:
                    sname = sname.strip()
                    if sname in stages:
                        existing = stages[sname].get("last_run")
                        if not existing or completed_iso > existing:
                            stages[sname]["last_run"] = completed_iso

        return {"ticker": ticker.upper(), "company_id": cid, "stages": stages}

    @post("/companies/{ticker:str}/run-pipeline")
    async def run_whole_pipeline(self, ticker: str, request: Request) -> dict:
        """Trigger ALL pipeline stages for a company in a single subprocess."""
        from sqlalchemy import text as sa_text

        company = get_company_by_ticker(ticker)
        if not company:
            return {"error": "Company not found"}

        all_stages = sorted(DAG.keys())
        stages_csv = ",".join(all_stages)
        subtask_label = "all"

        # Allow caller to request a full force-refresh
        body = await request.json() if request.content_type and "json" in (request.content_type or "") else {}
        force = body.get("force", False)

        run_id = f"web-{uuid.uuid4().hex[:12]}"
        cmd = [
            sys.executable, "scripts/run_pipeline.py",
            "--tickers", ticker.upper(),
            "--stages", "all",
            "--concurrency", "1",
            "--llm-concurrency", "5",
            "--run-id", run_id,
            "--include-inactive",
        ]
        if force:
            cmd.append("--force")
        log_path = f"/tmp/pipeline_{ticker.upper()}_all_{run_id}.log"

        with get_session() as session:
            active = session.execute(
                sa_text("""
                    SELECT COUNT(*) FROM pipeline_runs
                    WHERE :ticker = ANY(tickers_requested)
                      AND status IN ('running', 'enqueued')
                """),
                {"ticker": ticker.upper()},
            ).scalar()

            status_val = "enqueued" if active > 0 else "running"
            session.execute(
                sa_text("""
                    INSERT INTO pipeline_runs
                        (run_id, task, subtask, run_type, status, tickers_requested, parameters, started_at)
                    VALUES (:run_id, 'pipeline', :subtask, 'web_trigger', :status, :tickers,
                            :params, now())
                """),
                {
                    "run_id": run_id, "subtask": subtask_label,
                    "tickers": [ticker.upper()],
                    "status": status_val,
                    "params": json.dumps({
                        "cmd": cmd, "log_path": log_path,
                        "stages_requested": all_stages,
                        "source": "web_ui",
                    }),
                },
            )
            session.commit()

        if status_val == "enqueued":
            return {
                "ok": True,
                "run_id": run_id,
                "ticker": ticker.upper(),
                "status": "enqueued",
                "stages": all_stages,
                "message": f"Enqueued full pipeline for {ticker.upper()} (waiting for active run to finish)",
            }

        # Launch immediately
        log_file = open(log_path, "w")
        proc = subprocess.Popen(
            cmd, stdout=log_file, stderr=subprocess.STDOUT,
            cwd=str(Path(__file__).resolve().parent.parent),
        )
        _active_procs[run_id] = proc

        async def _wait_for_completion():
            loop = asyncio.get_event_loop()
            return_code = await loop.run_in_executor(None, proc.wait)
            log_file.close()
            _active_procs.pop(run_id, None)
            with get_session() as session:
                cur = session.execute(
                    sa_text("SELECT status FROM pipeline_runs WHERE run_id = :run_id"),
                    {"run_id": run_id},
                ).scalar()
                if cur != "cancelled":
                    status = RunStatus.COMPLETED if return_code == 0 else RunStatus.FAILED
                    session.execute(
                        sa_text("""
                            UPDATE pipeline_runs
                            SET status = :status, completed_at = now(),
                                error_message = CASE WHEN :status = 'failed' THEN 'Process exited with code ' || :rc ELSE NULL END
                            WHERE run_id = :run_id
                        """),
                        {"status": status.value, "run_id": run_id, "rc": str(return_code)},
                    )
                session.commit()
            await _launch_next_enqueued(ticker.upper())

        asyncio.create_task(_wait_for_completion())

        return {
            "ok": True,
            "run_id": run_id,
            "ticker": ticker.upper(),
            "status": "running",
            "stages": all_stages,
            "log_path": log_path,
            "message": f"Started full pipeline for {ticker.upper()} in background",
        }

    @post("/companies/{ticker:str}/run-stage")
    async def run_stage(self, ticker: str, request: Request) -> dict:
        """Trigger a pipeline stage. Validates against canonical DAG."""
        from sqlalchemy import text as sa_text

        body = await request.json()
        stage = body.get("stage")
        force = body.get("force", False)

        # Validate against the canonical DAG
        valid_stages = set(DAG.keys())
        # Keep backward compat: extract_evidence maps to extract_unified
        stage_arg = "extract_unified" if stage == "extract_evidence" else stage
        if stage_arg not in valid_stages:
            return {"error": f"Invalid stage: {stage}"}

        company = get_company_by_ticker(ticker)
        if not company:
            return {"error": "Company not found"}

        # Use --with-deps for dependency chaining (replaces EVIDENCE_CHAIN)
        all_stages = resolve_stages([stage_arg], with_deps=True)
        stages_list = sorted(all_stages)
        subtask_label = stage_arg

        # Build cmd and log_path
        run_id = f"web-{uuid.uuid4().hex[:12]}"
        cmd = [
            sys.executable, "scripts/run_pipeline.py",
            "--tickers", ticker.upper(),
            "--stages", *stages_list,
            "--with-deps",
            "--concurrency", "1",
            "--llm-concurrency", "5",
            "--run-id", run_id,
            "--include-inactive",
        ]
        if force:
            cmd.extend(["--force", *stages_list])
        log_path = f"/tmp/pipeline_{ticker.upper()}_{stage_arg}_{run_id}.log"

        # Check if any runs are already active (running or enqueued) for this ticker
        with get_session() as session:
            active = session.execute(
                sa_text("""
                    SELECT COUNT(*) FROM pipeline_runs
                    WHERE :ticker = ANY(tickers_requested)
                      AND status IN ('running', 'enqueued')
                """),
                {"ticker": ticker.upper()},
            ).scalar()

            if active > 0:
                # Enqueue behind running job
                session.execute(
                    sa_text("""
                        INSERT INTO pipeline_runs
                            (run_id, task, subtask, run_type, status, tickers_requested, parameters, started_at)
                        VALUES (:run_id, 'pipeline', :subtask, 'web_trigger', 'enqueued', :tickers,
                                :params, now())
                    """),
                    {
                        "run_id": run_id, "subtask": subtask_label,
                        "tickers": [ticker.upper()],
                        "params": json.dumps({
                            "cmd": cmd, "log_path": log_path,
                            "stages_requested": sorted(all_stages),
                            "source": "web_ui",
                        }),
                    },
                )
                session.commit()
                return {
                    "ok": True,
                    "run_id": run_id,
                    "stage": stage,
                    "ticker": ticker.upper(),
                    "status": "enqueued",
                    "message": f"Enqueued {stage} for {ticker.upper()} (waiting for active run to finish)",
                }

            # No active runs -- insert as running and launch immediately
            session.execute(
                sa_text("""
                    INSERT INTO pipeline_runs
                        (run_id, task, subtask, run_type, status, tickers_requested, parameters, started_at)
                    VALUES (:run_id, 'pipeline', :subtask, 'web_trigger', 'running', :tickers,
                            :params, now())
                """),
                {
                    "run_id": run_id, "subtask": subtask_label,
                    "tickers": [ticker.upper()],
                    "params": json.dumps({
                        "cmd": cmd, "log_path": log_path,
                        "stages_requested": sorted(all_stages),
                        "source": "web_ui",
                    }),
                },
            )
            session.commit()

        # Launch as background subprocess
        log_file = open(log_path, "w")
        proc = subprocess.Popen(
            cmd, stdout=log_file, stderr=subprocess.STDOUT,
            cwd=str(Path(__file__).resolve().parent.parent),
        )
        _active_procs[run_id] = proc

        # Fire-and-forget: update pipeline_run when process completes
        async def _wait_for_completion():
            loop = asyncio.get_event_loop()
            return_code = await loop.run_in_executor(None, proc.wait)
            log_file.close()
            _active_procs.pop(run_id, None)
            # If already cancelled by user, don't overwrite status
            with get_session() as session:
                cur = session.execute(
                    sa_text("SELECT status FROM pipeline_runs WHERE run_id = :run_id"),
                    {"run_id": run_id},
                ).scalar()
                if cur != "cancelled":
                    status = RunStatus.COMPLETED if return_code == 0 else RunStatus.FAILED
                    session.execute(
                        sa_text("""
                            UPDATE pipeline_runs
                            SET status = :status, completed_at = now(),
                                error_message = CASE WHEN :status = 'failed' THEN 'Process exited with code ' || :rc ELSE NULL END
                            WHERE run_id = :run_id
                        """),
                        {"status": status.value, "run_id": run_id, "rc": str(return_code)},
                    )
                session.commit()
            # Drain queue: launch next enqueued job for this ticker
            await _launch_next_enqueued(ticker.upper())

        asyncio.create_task(_wait_for_completion())

        return {
            "ok": True,
            "run_id": run_id,
            "stage": stage,
            "ticker": ticker.upper(),
            "log_path": log_path,
            "message": f"Started {stage} for {ticker.upper()} in background",
        }

    @post("/companies/{ticker:str}/cancel-run")
    async def cancel_run(self, ticker: str, request: Request) -> dict:
        """Cancel a running or enqueued pipeline run."""
        from sqlalchemy import text as sa_text

        body = await request.json()
        run_id = body.get("run_id")
        if not run_id:
            return {"error": "run_id is required"}

        with get_session() as session:
            row = session.execute(
                sa_text("""
                    SELECT status FROM pipeline_runs
                    WHERE run_id = :run_id
                      AND :ticker = ANY(tickers_requested)
                """),
                {"run_id": run_id, "ticker": ticker.upper()},
            ).fetchone()

            if not row:
                return {"error": "Run not found"}

            previous_status = row[0]
            if previous_status not in ("running", "enqueued"):
                return {"error": f"Cannot cancel run with status '{previous_status}'"}

            session.execute(
                sa_text("""
                    UPDATE pipeline_runs
                    SET status = 'cancelled', completed_at = now(),
                        error_message = 'Cancelled by user'
                    WHERE run_id = :run_id
                """),
                {"run_id": run_id},
            )
            session.commit()

        # If running, terminate the subprocess
        if previous_status == "running":
            proc = _active_procs.pop(run_id, None)
            if proc:
                proc.terminate()

        # If we cancelled a running job, promote next enqueued
        if previous_status == "running":
            asyncio.create_task(_launch_next_enqueued(ticker.upper()))

        return {"ok": True, "run_id": run_id, "previous_status": previous_status}

    @get("/companies/{ticker:str}/runs")
    async def runs(self, ticker: str, request: Request) -> dict:
        """List recent pipeline runs targeted at this company (excludes bulk runs)."""
        from sqlalchemy import text as sa_text

        company = get_company_by_ticker(ticker)
        if not company:
            return {"error": "Company not found"}

        page = int(request.query_params.get("page", 1))
        per_page = int(request.query_params.get("per_page", 5))
        offset = (page - 1) * per_page

        with get_session() as session:
            r = session.execute(
                sa_text("""
                    SELECT run_id, task, subtask, run_type, status,
                           started_at, completed_at, error_message,
                           tickers_succeeded, tickers_failed, parent_run_id
                    FROM pipeline_runs
                    WHERE :ticker = ANY(tickers_requested)
                      AND array_length(tickers_requested, 1) <= 50
                    ORDER BY started_at DESC
                    LIMIT :lim OFFSET :off
                """),
                {"ticker": ticker.upper(), "lim": per_page + 1, "off": offset},
            )
            rows = r.fetchall()
            has_more = len(rows) > per_page
            rows = rows[:per_page]

            runs = []
            for row in rows:
                started = row[5]
                completed = row[6]
                duration_sec = None
                if started and completed:
                    duration_sec = round((completed - started).total_seconds(), 1)

                runs.append({
                    "run_id": row[0],
                    "task": row[1],
                    "subtask": row[2],
                    "run_type": row[3],
                    "status": row[4],
                    "started_at": started.isoformat() if started else None,
                    "completed_at": completed.isoformat() if completed else None,
                    "duration_sec": duration_sec,
                    "error_message": row[7],
                    "tickers_succeeded": row[8],
                    "tickers_failed": row[9],
                    "parent_run_id": row[10],
                })

            # Enrich runs with result counts from downstream tables
            run_ids = [r["run_id"] for r in runs]
            if run_ids:
                counts_result = session.execute(
                    sa_text("""
                        SELECT
                            pr.run_id,
                            COALESCE(e.cnt, 0)  AS evidence_count,
                            COALESCE(eg.cnt, 0) AS groups_count,
                            COALESCE(v.cnt, 0)  AS valuations_count,
                            COALESCE(cs.cnt, 0) AS scores_count
                        FROM pipeline_runs pr
                        LEFT JOIN (SELECT pipeline_run_id, count(*) cnt FROM evidence       GROUP BY 1) e  ON e.pipeline_run_id = pr.id
                        LEFT JOIN (SELECT pipeline_run_id, count(*) cnt FROM evidence_groups GROUP BY 1) eg ON eg.pipeline_run_id = pr.id
                        LEFT JOIN (SELECT pipeline_run_id, count(*) cnt FROM valuations WHERE stage = 'final' GROUP BY 1) v ON v.pipeline_run_id = pr.id
                        LEFT JOIN (SELECT pipeline_run_id, count(*) cnt FROM company_scores  GROUP BY 1) cs ON cs.pipeline_run_id = pr.id
                        WHERE pr.run_id = ANY(:run_ids)
                    """),
                    {"run_ids": run_ids},
                )
                counts_by_run = {
                    r[0]: {
                        "evidence_count": r[1],
                        "groups_count": r[2],
                        "valuations_count": r[3],
                        "scores_count": r[4],
                    }
                    for r in counts_result.fetchall()
                }
                for run in runs:
                    run.update(counts_by_run.get(run["run_id"], {
                        "evidence_count": 0,
                        "groups_count": 0,
                        "valuations_count": 0,
                        "scores_count": 0,
                    }))

        return {"ticker": ticker.upper(), "runs": runs, "page": page, "per_page": per_page, "has_more": has_more}


async def _launch_next_enqueued(ticker: str) -> None:
    """Claim and launch the oldest enqueued pipeline run for this ticker."""
    from sqlalchemy import text as sa_text

    with get_session() as session:
        # Atomically claim the oldest enqueued run for this ticker
        row = session.execute(
            sa_text("""
                UPDATE pipeline_runs
                SET status = 'running'
                WHERE id = (
                    SELECT id FROM pipeline_runs
                    WHERE :ticker = ANY(tickers_requested)
                      AND status = 'enqueued'
                    ORDER BY id
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING run_id, parameters
            """),
            {"ticker": ticker},
        ).fetchone()
        session.commit()

    if row is None:
        return

    next_run_id = row[0]
    params = row[1] or {}
    cmd = params.get("cmd")
    log_path = params.get("log_path")

    if not cmd or not log_path:
        # Cannot launch -- mark as failed
        with get_session() as session:
            session.execute(
                sa_text("""
                    UPDATE pipeline_runs
                    SET status = 'failed', completed_at = now(),
                        error_message = 'Missing cmd or log_path in parameters'
                    WHERE run_id = :run_id
                """),
                {"run_id": next_run_id},
            )
            session.commit()
        await _launch_next_enqueued(ticker)
        return

    log_file = open(log_path, "w")
    proc = subprocess.Popen(
        cmd, stdout=log_file, stderr=subprocess.STDOUT,
        cwd=str(Path(__file__).resolve().parent.parent),
    )
    _active_procs[next_run_id] = proc

    async def _wait_and_drain():
        loop = asyncio.get_event_loop()
        return_code = await loop.run_in_executor(None, proc.wait)
        log_file.close()
        _active_procs.pop(next_run_id, None)
        # If already cancelled by user, don't overwrite status
        with get_session() as session:
            cur = session.execute(
                sa_text("SELECT status FROM pipeline_runs WHERE run_id = :run_id"),
                {"run_id": next_run_id},
            ).scalar()
            if cur != "cancelled":
                status = RunStatus.COMPLETED if return_code == 0 else RunStatus.FAILED
                session.execute(
                    sa_text("""
                        UPDATE pipeline_runs
                        SET status = :status, completed_at = now(),
                            error_message = CASE WHEN :status = 'failed' THEN 'Process exited with code ' || :rc ELSE NULL END
                        WHERE run_id = :run_id
                    """),
                    {"status": status.value, "run_id": next_run_id, "rc": str(return_code)},
                )
            session.commit()
        # Recursively drain the queue
        await _launch_next_enqueued(ticker)

    asyncio.create_task(_wait_and_drain())
