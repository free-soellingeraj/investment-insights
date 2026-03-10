#!/usr/bin/env python3
"""LLM Model Comparison Experiment.

Compares candidate workhorse models against Gemini 2.5 Flash on the three
pipeline tasks:
  1. Evidence extraction (filing + news)
  2. Dollar estimation
  3. Link classification

For each task, runs a fixed set of real examples from the DB through each
candidate model and measures:
  - Structured-output parse success rate
  - Latency (p50, p95)
  - Output quality (agreement with Gemini 2.5 Flash baseline)
  - Cost estimate per 1K calls

Usage:
    python scripts/llm_model_experiment.py [--tasks extract estimate classify] [--samples 5]

Results are written to data/llm_experiment_results.json and printed as a table.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from statistics import median

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import os

from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_ai.models.google import GoogleModel

from ai_opportunity_index.config import (
    get_google_provider,
    PROJECT_ROOT,
    DATA_DIR,
)
from ai_opportunity_index.prompts.loader import load_prompt
from ai_opportunity_index.scoring.pipeline.llm_extractors import ExtractedPassages
from ai_opportunity_index.scoring.pipeline.llm_estimators import DollarEstimate
from ai_opportunity_index.storage.db import get_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Candidate models ─────────────────────────────────────────────────────
# Prefix "groq:" for Groq models, plain names for Gemini

CANDIDATE_MODELS = [
    # Baseline
    "gemini-2.5-flash",
    # Groq (free tier)
    "groq:llama-3.3-70b-versatile",
    "groq:llama-3.1-8b-instant",
    "groq:meta-llama/llama-4-scout-17b-16e-instruct",
    "groq:qwen/qwen3-32b",
    "groq:openai/gpt-oss-120b",
    "groq:moonshotai/kimi-k2-instruct-0905",
]

GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")

# Rough cost per 1M input tokens (USD) — update as pricing changes
MODEL_COST_PER_1M_INPUT = {
    "gemini-2.5-flash": 0.15,
    "gemini-2.0-flash": 0.10,
    "gemini-2.5-pro": 1.25,
    # Groq free tier = $0, paid tier prices below
    "groq:llama-3.3-70b-versatile": 0.59,
    "groq:llama-3.1-8b-instant": 0.05,
    "groq:meta-llama/llama-4-scout-17b-16e-instruct": 0.20,
    "groq:qwen/qwen3-32b": 0.29,
    "groq:openai/gpt-oss-120b": 0.59,
    "groq:moonshotai/kimi-k2-instruct-0905": 0.45,
}

MODEL_COST_PER_1M_OUTPUT = {
    "gemini-2.5-flash": 0.60,
    "gemini-2.0-flash": 0.40,
    "gemini-2.5-pro": 10.00,
    "groq:llama-3.3-70b-versatile": 0.79,
    "groq:llama-3.1-8b-instant": 0.08,
    "groq:meta-llama/llama-4-scout-17b-16e-instruct": 0.20,
    "groq:qwen/qwen3-32b": 0.39,
    "groq:openai/gpt-oss-120b": 3.00,
    "groq:moonshotai/kimi-k2-instruct-0905": 0.55,
}


# ── Groq client helper ──────────────────────────────────────────────────

_groq_client = None

def get_groq_client():
    """Lazy-init OpenAI client pointing at Groq."""
    global _groq_client
    if _groq_client is None:
        from openai import AsyncOpenAI
        _groq_client = AsyncOpenAI(
            base_url="https://api.groq.com/openai/v1",
            api_key=GROQ_API_KEY,
        )
    return _groq_client


async def groq_json_call(model_id: str, prompt: str, schema: dict, system_prompt: str = "") -> tuple[dict, int, int]:
    """Call a Groq model with JSON mode, parse response, return (parsed_dict, in_tokens, out_tokens)."""
    client = get_groq_client()
    schema_str = json.dumps(schema, indent=2)

    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({
        "role": "user",
        "content": f"{prompt}\n\n## Output Format\nReturn ONLY a valid JSON object matching this schema:\n```json\n{schema_str}\n```\nReturn raw JSON only — no markdown fences, no explanation.",
    })

    resp = await client.chat.completions.create(
        model=model_id,
        messages=messages,
        response_format={"type": "json_object"},
        temperature=0,
    )
    content = resp.choices[0].message.content
    parsed = json.loads(content)
    return parsed, resp.usage.prompt_tokens, resp.usage.completion_tokens


def is_groq_model(model_name: str) -> bool:
    return model_name.startswith("groq:")


# ── Data classes ─────────────────────────────────────────────────────────

@dataclass
class TrialResult:
    model: str
    task: str
    sample_id: str
    success: bool
    latency_s: float
    error: str = ""
    output_json: dict = field(default_factory=dict)
    input_tokens: int = 0
    output_tokens: int = 0


@dataclass
class ModelSummary:
    model: str
    task: str
    n_samples: int
    success_rate: float
    latency_p50: float
    latency_p95: float
    avg_input_tokens: int
    avg_output_tokens: int
    est_cost_per_1k: float  # USD per 1K calls
    agreement_with_baseline: float  # 0-1, how often agrees with gemini-2.5-flash


# ── Sample fetching ──────────────────────────────────────────────────────

def fetch_extraction_samples(n: int) -> list[dict]:
    """Fetch N real evidence passages from DB for extraction testing."""
    from sqlalchemy import text
    s = get_session()
    try:
        rows = s.execute(text("""
            SELECT DISTINCT ON (c.ticker)
                c.ticker, c.company_name, c.sector,
                ep.passage_text, ep.source_url
            FROM companies c
            JOIN evidence_groups eg ON eg.company_id = c.id
            JOIN evidence_group_passages ep ON ep.group_id = eg.id
            WHERE ep.passage_text IS NOT NULL
              AND LENGTH(ep.passage_text) > 200
            ORDER BY c.ticker, RANDOM()
            LIMIT :n
        """), {"n": n}).mappings().fetchall()

        samples = []
        for r in rows:
            samples.append({
                "id": r["ticker"],
                "company_name": r["company_name"] or r["ticker"],
                "ticker": r["ticker"],
                "sector": r["sector"] or "Technology",
                "text": r["passage_text"][:4000],
                "source_url": r["source_url"] or "",
            })
        return samples
    finally:
        s.close()


def fetch_estimation_samples(n: int) -> list[dict]:
    """Fetch N evidence groups with existing valuations for estimation testing."""
    from sqlalchemy import text
    s = get_session()
    try:
        rows = s.execute(text("""
            SELECT DISTINCT ON (c.ticker)
                c.ticker, c.company_name, c.sector,
                eg.target_dimension,
                ep.passage_text
            FROM companies c
            JOIN evidence_groups eg ON eg.company_id = c.id
            JOIN evidence_group_passages ep ON ep.group_id = eg.id
            JOIN valuations v ON v.group_id = eg.id
            WHERE ep.passage_text IS NOT NULL
              AND v.dollar_low IS NOT NULL
            ORDER BY c.ticker, RANDOM()
            LIMIT :n
        """), {"n": n}).mappings().fetchall()

        samples = []
        for r in rows:
            samples.append({
                "id": r["ticker"],
                "company_name": r["company_name"] or r["ticker"],
                "ticker": r["ticker"],
                "sector": r["sector"] or "Technology",
                "revenue": 0,
                "employees": 0,
                "passage_text": r["passage_text"][:2000],
                "target_dimension": r["target_dimension"] or "general",
            })
        return samples
    finally:
        s.close()


def fetch_classification_samples(n: int) -> list[dict]:
    """Fetch N news articles/URLs for classification testing."""
    from sqlalchemy import text
    s = get_session()
    try:
        rows = s.execute(text("""
            SELECT DISTINCT ON (c.ticker)
                c.ticker, c.company_name,
                ep.passage_text, ep.source_url
            FROM companies c
            JOIN evidence_groups eg ON eg.company_id = c.id
            JOIN evidence_group_passages ep ON ep.group_id = eg.id
            WHERE ep.source_url IS NOT NULL
              AND ep.passage_text IS NOT NULL
              AND LENGTH(ep.passage_text) > 100
            ORDER BY c.ticker, RANDOM()
            LIMIT :n
        """), {"n": n}).mappings().fetchall()

        samples = []
        for r in rows:
            samples.append({
                "id": r["ticker"],
                "company_name": r["company_name"] or r["ticker"],
                "ticker": r["ticker"],
                "text": r["passage_text"][:2000],
                "url": r["source_url"],
            })
        return samples
    finally:
        s.close()


# ── Trial runners ────────────────────────────────────────────────────────

EXTRACTION_SCHEMA = {
    "type": "object",
    "properties": {
        "passages": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "passage_text": {"type": "string"},
                    "target_dimension": {"type": "string", "enum": ["cost", "revenue", "general"]},
                    "capture_stage": {"type": "string", "enum": ["plan", "investment", "capture"]},
                    "confidence": {"type": "number"},
                    "reasoning": {"type": "string"},
                },
                "required": ["passage_text", "target_dimension", "capture_stage", "confidence", "reasoning"],
            },
        },
    },
    "required": ["passages"],
}

ESTIMATION_SCHEMA = {
    "type": "object",
    "properties": {
        "annual_dollar_impact": {"type": "number"},
        "year_1_pct": {"type": "number"},
        "year_2_pct": {"type": "number"},
        "year_3_pct": {"type": "number"},
        "horizon_shape": {"type": "string", "enum": ["front_loaded", "linear", "back_loaded"]},
        "rationale": {"type": "string"},
    },
    "required": ["annual_dollar_impact", "year_1_pct", "year_2_pct", "year_3_pct", "horizon_shape", "rationale"],
}

CLASSIFICATION_SCHEMA = {
    "type": "object",
    "properties": {
        "is_ai_related": {"type": "boolean"},
        "confidence": {"type": "number"},
        "reasoning": {"type": "string"},
    },
    "required": ["is_ai_related", "confidence", "reasoning"],
}


async def run_extraction_trial(model_name: str, sample: dict) -> TrialResult:
    """Run one extraction trial for a model."""
    prompt = load_prompt(
        "extract_news_evidence",
        company_name=sample["company_name"],
        ticker=sample["ticker"],
        sector=sample["sector"],
        revenue=0,
        document_text=sample["text"],
    )

    t0 = time.monotonic()
    try:
        if is_groq_model(model_name):
            groq_id = model_name.removeprefix("groq:")
            parsed, in_tok, out_tok = await groq_json_call(groq_id, prompt, EXTRACTION_SCHEMA)
            elapsed = time.monotonic() - t0
            passages = parsed.get("passages", [])
            return TrialResult(
                model=model_name, task="extract", sample_id=sample["id"],
                success=True, latency_s=round(elapsed, 3),
                output_json={"n_passages": len(passages), "passages": passages[:3]},
                input_tokens=in_tok, output_tokens=out_tok,
            )
        else:
            provider = get_google_provider()
            model = GoogleModel(model_name, provider=provider)
            agent = Agent(model, output_type=ExtractedPassages)
            result = await agent.run(prompt)
            elapsed = time.monotonic() - t0
            output = result.output
            usage = result.usage()
            return TrialResult(
                model=model_name, task="extract", sample_id=sample["id"],
                success=True, latency_s=round(elapsed, 3),
                output_json={"n_passages": len(output.passages), "passages": [p.model_dump() for p in output.passages[:3]]},
                input_tokens=getattr(usage, "input_tokens", 0) or 0,
                output_tokens=getattr(usage, "output_tokens", 0) or 0,
            )
    except Exception as e:
        elapsed = time.monotonic() - t0
        return TrialResult(
            model=model_name, task="extract", sample_id=sample["id"],
            success=False, latency_s=round(elapsed, 3), error=str(e)[:300],
        )


async def run_estimation_trial(model_name: str, sample: dict) -> TrialResult:
    """Run one estimation trial for a model."""
    prompt = load_prompt(
        "estimate_dollar_impact",
        company_name=sample["company_name"],
        revenue=sample["revenue"],
        employees=sample["employees"],
        sector=sample["sector"],
        target_dimension=sample["target_dimension"],
        capture_stage="invested",
        passage_text=sample["passage_text"],
    )
    system_prompt = (
        "You are a financial analyst who estimates the annual dollar impact "
        "of AI initiatives for public companies. Be conservative and realistic."
    )

    t0 = time.monotonic()
    try:
        if is_groq_model(model_name):
            groq_id = model_name.removeprefix("groq:")
            parsed, in_tok, out_tok = await groq_json_call(groq_id, prompt, ESTIMATION_SCHEMA, system_prompt)
            elapsed = time.monotonic() - t0
            return TrialResult(
                model=model_name, task="estimate", sample_id=sample["id"],
                success=True, latency_s=round(elapsed, 3),
                output_json=parsed, input_tokens=in_tok, output_tokens=out_tok,
            )
        else:
            provider = get_google_provider()
            model = GoogleModel(model_name, provider=provider)
            agent = Agent(model, output_type=DollarEstimate, system_prompt=system_prompt)
            result = await agent.run(prompt)
            elapsed = time.monotonic() - t0
            output = result.output
            usage = result.usage()
            return TrialResult(
                model=model_name, task="estimate", sample_id=sample["id"],
                success=True, latency_s=round(elapsed, 3),
                output_json=output.model_dump(),
                input_tokens=getattr(usage, "input_tokens", 0) or 0,
                output_tokens=getattr(usage, "output_tokens", 0) or 0,
            )
    except Exception as e:
        elapsed = time.monotonic() - t0
        return TrialResult(
            model=model_name, task="estimate", sample_id=sample["id"],
            success=False, latency_s=round(elapsed, 3), error=str(e)[:300],
        )


class ClassificationResult(BaseModel):
    """Is this article about AI initiatives?"""
    is_ai_related: bool
    confidence: float
    reasoning: str


async def run_classification_trial(model_name: str, sample: dict) -> TrialResult:
    """Run one classification trial for a model."""
    prompt = (
        f"Classify whether this article about {sample['company_name']} ({sample['ticker']}) "
        f"is related to AI/ML initiatives.\n\n"
        f"URL: {sample.get('url', 'N/A')}\n\n"
        f"Text:\n{sample['text']}"
    )

    t0 = time.monotonic()
    try:
        if is_groq_model(model_name):
            groq_id = model_name.removeprefix("groq:")
            parsed, in_tok, out_tok = await groq_json_call(groq_id, prompt, CLASSIFICATION_SCHEMA)
            elapsed = time.monotonic() - t0
            return TrialResult(
                model=model_name, task="classify", sample_id=sample["id"],
                success=True, latency_s=round(elapsed, 3),
                output_json=parsed, input_tokens=in_tok, output_tokens=out_tok,
            )
        else:
            provider = get_google_provider()
            model = GoogleModel(model_name, provider=provider)
            agent = Agent(model, output_type=ClassificationResult)
            result = await agent.run(prompt)
            elapsed = time.monotonic() - t0
            output = result.output
            usage = result.usage()
            return TrialResult(
                model=model_name, task="classify", sample_id=sample["id"],
                success=True, latency_s=round(elapsed, 3),
                output_json=output.model_dump(),
                input_tokens=getattr(usage, "input_tokens", 0) or 0,
                output_tokens=getattr(usage, "output_tokens", 0) or 0,
            )
    except Exception as e:
        elapsed = time.monotonic() - t0
        return TrialResult(
            model=model_name, task="classify", sample_id=sample["id"],
            success=False, latency_s=round(elapsed, 3), error=str(e)[:300],
        )


# ── Orchestrator ─────────────────────────────────────────────────────────

TASK_RUNNERS = {
    "extract": (fetch_extraction_samples, run_extraction_trial),
    "estimate": (fetch_estimation_samples, run_estimation_trial),
    "classify": (fetch_classification_samples, run_classification_trial),
}


def compute_agreement(baseline_results: list[TrialResult], candidate_results: list[TrialResult], task: str) -> float:
    """Compare candidate outputs to baseline (gemini-2.5-flash)."""
    baseline_map = {r.sample_id: r for r in baseline_results if r.success}
    agreements = 0
    comparisons = 0

    for cr in candidate_results:
        if not cr.success or cr.sample_id not in baseline_map:
            continue
        br = baseline_map[cr.sample_id]
        comparisons += 1

        if task == "classify":
            # Binary agreement
            if cr.output_json.get("is_ai_related") == br.output_json.get("is_ai_related"):
                agreements += 1
        elif task == "extract":
            # Agreement on passage count (within 50%)
            bc = br.output_json.get("n_passages", 0)
            cc = cr.output_json.get("n_passages", 0)
            if bc == 0 and cc == 0:
                agreements += 1
            elif bc > 0 and abs(cc - bc) / bc <= 0.5:
                agreements += 1
        elif task == "estimate":
            # Agreement on dollar estimate order of magnitude
            bd = br.output_json.get("annual_dollar_impact", 0)
            cd = cr.output_json.get("annual_dollar_impact", 0)
            if bd == 0 and cd == 0:
                agreements += 1
            elif bd != 0 and cd != 0:
                import math
                if abs(math.log10(max(abs(bd), 1)) - math.log10(max(abs(cd), 1))) <= 1.0:
                    agreements += 1

    return agreements / max(comparisons, 1)


def summarize(model: str, task: str, results: list[TrialResult], agreement: float) -> ModelSummary:
    """Compute summary statistics for a model's results on a task."""
    successes = [r for r in results if r.success]
    latencies = sorted([r.latency_s for r in successes]) if successes else [0]

    avg_in = int(sum(r.input_tokens for r in successes) / max(len(successes), 1))
    avg_out = int(sum(r.output_tokens for r in successes) / max(len(successes), 1))

    cost_in = MODEL_COST_PER_1M_INPUT.get(model, 0.15)
    cost_out = MODEL_COST_PER_1M_OUTPUT.get(model, 0.60)
    cost_per_call = (avg_in * cost_in + avg_out * cost_out) / 1_000_000
    cost_per_1k = cost_per_call * 1000

    p50_idx = len(latencies) // 2
    p95_idx = min(int(len(latencies) * 0.95), len(latencies) - 1)

    return ModelSummary(
        model=model,
        task=task,
        n_samples=len(results),
        success_rate=len(successes) / max(len(results), 1),
        latency_p50=latencies[p50_idx],
        latency_p95=latencies[p95_idx],
        avg_input_tokens=avg_in,
        avg_output_tokens=avg_out,
        est_cost_per_1k=round(cost_per_1k, 4),
        agreement_with_baseline=round(agreement, 3),
    )


async def run_experiment(tasks: list[str], n_samples: int, models: list[str]):
    """Run the full experiment."""
    all_results: dict[str, dict[str, list[TrialResult]]] = {}  # task -> model -> results
    summaries: list[ModelSummary] = []

    for task in tasks:
        if task not in TASK_RUNNERS:
            logger.warning("Unknown task: %s", task)
            continue

        fetch_fn, run_fn = TASK_RUNNERS[task]
        logger.info("━━━ Task: %s ━━━", task)
        logger.info("Fetching %d samples...", n_samples)
        samples = fetch_fn(n_samples)
        if not samples:
            logger.warning("No samples found for task %s, skipping", task)
            continue
        logger.info("Got %d samples", len(samples))

        all_results[task] = {}

        for model_name in models:
            logger.info("  Model: %s", model_name)
            results = []
            for i, sample in enumerate(samples):
                logger.info("    Sample %d/%d: %s", i + 1, len(samples), sample["id"])
                # Rate limit: stagger calls slightly
                if i > 0:
                    await asyncio.sleep(1.0)
                result = await run_fn(model_name, sample)
                results.append(result)
                status = "OK" if result.success else f"FAIL: {result.error[:80]}"
                logger.info("      %s (%.1fs)", status, result.latency_s)

            all_results[task][model_name] = results

        # Compute agreements
        baseline_key = "gemini-2.5-flash"
        baseline_results = all_results[task].get(baseline_key, [])

        for model_name in models:
            results = all_results[task][model_name]
            if model_name == baseline_key:
                agreement = 1.0
            else:
                agreement = compute_agreement(baseline_results, results, task)
            summary = summarize(model_name, task, results, agreement)
            summaries.append(summary)

    return summaries, all_results


def print_results(summaries: list[ModelSummary]):
    """Print results as a formatted table."""
    print("\n" + "=" * 110)
    print("LLM MODEL COMPARISON RESULTS")
    print("=" * 110)

    tasks = sorted(set(s.task for s in summaries))
    for task in tasks:
        task_summaries = [s for s in summaries if s.task == task]
        print(f"\n{'─' * 110}")
        print(f"  Task: {task.upper()}")
        print(f"{'─' * 110}")
        print(f"  {'Model':<25} {'Success':>8} {'P50(s)':>8} {'P95(s)':>8} {'Agree':>8} {'$/1K':>10} {'AvgIn':>8} {'AvgOut':>8}")
        print(f"  {'─'*25} {'─'*8} {'─'*8} {'─'*8} {'─'*8} {'─'*10} {'─'*8} {'─'*8}")

        for s in sorted(task_summaries, key=lambda x: x.est_cost_per_1k):
            agree_str = f"{s.agreement_with_baseline:.0%}"
            success_str = f"{s.success_rate:.0%}"
            print(
                f"  {s.model:<25} {success_str:>8} {s.latency_p50:>7.1f}s {s.latency_p95:>7.1f}s "
                f"{agree_str:>8} ${s.est_cost_per_1k:>8.4f} {s.avg_input_tokens:>8} {s.avg_output_tokens:>8}"
            )

    print(f"\n{'=' * 110}")
    print("RECOMMENDATION")
    print("=" * 110)

    # Find best value: high agreement, low cost, high success
    for task in tasks:
        task_summaries = [s for s in summaries if s.task == task and s.success_rate >= 0.6]
        if not task_summaries:
            print(f"  {task}: No viable candidates (all <60% success rate)")
            continue

        # Score: agreement * success_rate / (cost + 0.001) — higher is better
        best = max(task_summaries, key=lambda s: s.agreement_with_baseline * s.success_rate / (s.est_cost_per_1k + 0.001))
        cheapest = min(task_summaries, key=lambda s: s.est_cost_per_1k)
        fastest = min(task_summaries, key=lambda s: s.latency_p50)

        print(f"  {task}:")
        print(f"    Best value:   {best.model} (agree={best.agreement_with_baseline:.0%}, ${best.est_cost_per_1k:.4f}/1K)")
        print(f"    Cheapest:     {cheapest.model} (${cheapest.est_cost_per_1k:.4f}/1K, agree={cheapest.agreement_with_baseline:.0%})")
        print(f"    Fastest:      {fastest.model} (p50={fastest.latency_p50:.1f}s)")

    print()


def main():
    parser = argparse.ArgumentParser(description="LLM Model Comparison Experiment")
    parser.add_argument("--tasks", nargs="+", default=["extract", "estimate", "classify"],
                        choices=["extract", "estimate", "classify"],
                        help="Tasks to benchmark")
    parser.add_argument("--samples", type=int, default=5,
                        help="Number of samples per task (default: 5)")
    parser.add_argument("--models", nargs="+", default=None,
                        help="Models to test (default: all candidates)")
    args = parser.parse_args()

    models = args.models or CANDIDATE_MODELS

    logger.info("Starting LLM model experiment")
    logger.info("  Tasks: %s", args.tasks)
    logger.info("  Samples: %d per task", args.samples)
    logger.info("  Models: %s", models)

    summaries, all_results = asyncio.run(
        run_experiment(args.tasks, args.samples, models)
    )

    print_results(summaries)

    # Save detailed results
    output_path = DATA_DIR / "llm_experiment_results.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    serializable = {
        "summaries": [asdict(s) for s in summaries],
        "trials": {
            task: {
                model: [asdict(r) for r in results]
                for model, results in model_results.items()
            }
            for task, model_results in all_results.items()
        },
    }
    output_path.write_text(json.dumps(serializable, indent=2, default=str))
    logger.info("Detailed results saved to %s", output_path)


if __name__ == "__main__":
    main()
