"""Cost and usage tracker for data collection and scoring pipeline.

Tracks API calls, data volumes, timing, and estimated costs.
Writes a running log to data/cost_log.jsonl and a summary to data/cost_summary.json.
"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path

from ai_opportunity_index.config import DATA_DIR

logger = logging.getLogger(__name__)

COST_LOG_PATH = DATA_DIR / "cost_log.jsonl"
COST_SUMMARY_PATH = DATA_DIR / "cost_summary.json"

# Estimated costs per API call (USD)
COST_PER_CALL = {
    "yahoo_finance": 0.0,      # free
    "sec_edgar": 0.0,           # free
    "anthropic_llm": 0.003,    # ~$3/1M input tokens, ~15k chars per filing ≈ $0.003
    "openai_llm": 0.001,       # gpt-4o-mini
    "gnews_api": 0.0,           # free tier
    "adzuna_api": 0.0,          # free tier
    "patentsview_api": 0.0,     # free
}


class CostTracker:
    """Tracks API usage and costs for a data collection run."""

    def __init__(self, run_name: str):
        self.run_name = run_name
        self.started_at = datetime.utcnow()
        self.counters: dict[str, int] = {}
        self.bytes_downloaded: dict[str, int] = {}
        self.errors: dict[str, int] = {}
        self.timings: dict[str, float] = {}
        self._timer_starts: dict[str, float] = {}

    def record_call(self, api: str, bytes_size: int = 0, error: bool = False):
        """Record an API call."""
        self.counters[api] = self.counters.get(api, 0) + 1
        self.bytes_downloaded[api] = self.bytes_downloaded.get(api, 0) + bytes_size
        if error:
            self.errors[api] = self.errors.get(api, 0) + 1

    def start_timer(self, label: str):
        """Start a named timer."""
        self._timer_starts[label] = time.time()

    def stop_timer(self, label: str):
        """Stop a named timer and accumulate elapsed time."""
        if label in self._timer_starts:
            elapsed = time.time() - self._timer_starts.pop(label)
            self.timings[label] = self.timings.get(label, 0) + elapsed

    def estimated_cost(self) -> dict[str, float]:
        """Calculate estimated costs per API."""
        costs = {}
        for api, count in self.counters.items():
            per_call = COST_PER_CALL.get(api, 0.0)
            costs[api] = round(count * per_call, 4)
        return costs

    def summary(self) -> dict:
        """Generate a summary dict."""
        costs = self.estimated_cost()
        elapsed = (datetime.utcnow() - self.started_at).total_seconds()
        return {
            "run_name": self.run_name,
            "started_at": self.started_at.isoformat(),
            "elapsed_seconds": round(elapsed, 1),
            "api_calls": dict(self.counters),
            "api_errors": dict(self.errors),
            "bytes_downloaded": dict(self.bytes_downloaded),
            "mb_downloaded": {
                k: round(v / (1024 * 1024), 2)
                for k, v in self.bytes_downloaded.items()
            },
            "estimated_cost_usd": costs,
            "total_estimated_cost_usd": round(sum(costs.values()), 4),
            "timings_seconds": {k: round(v, 1) for k, v in self.timings.items()},
        }

    def log_event(self, event: str, details: dict | None = None):
        """Append an event to the JSONL log."""
        entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "run_name": self.run_name,
            "event": event,
        }
        if details:
            entry.update(details)

        COST_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(COST_LOG_PATH, "a") as f:
            f.write(json.dumps(entry) + "\n")

    def save_summary(self):
        """Save the current summary to disk."""
        s = self.summary()
        COST_SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)

        # Append to summaries list
        summaries = []
        if COST_SUMMARY_PATH.exists():
            try:
                summaries = json.loads(COST_SUMMARY_PATH.read_text())
                if not isinstance(summaries, list):
                    summaries = [summaries]
            except Exception:
                summaries = []
        summaries.append(s)
        COST_SUMMARY_PATH.write_text(json.dumps(summaries, indent=2))
        logger.info("Cost summary saved: %s", json.dumps(s, indent=2))

    def print_summary(self):
        """Print a human-readable summary."""
        s = self.summary()
        print(f"\n{'=' * 60}")
        print(f"  Cost Report: {s['run_name']}")
        print(f"{'=' * 60}")
        print(f"  Duration: {s['elapsed_seconds']:.0f}s ({s['elapsed_seconds']/60:.1f} min)")
        print()
        print("  API Calls:")
        for api, count in s["api_calls"].items():
            errors = s["api_errors"].get(api, 0)
            mb = s["mb_downloaded"].get(api, 0)
            cost = s["estimated_cost_usd"].get(api, 0)
            err_str = f" ({errors} errors)" if errors else ""
            print(f"    {api:25s} {count:6d} calls, {mb:8.2f} MB, ${cost:.4f}{err_str}")
        print()
        print(f"  Total estimated cost: ${s['total_estimated_cost_usd']:.4f}")
        print(f"{'=' * 60}\n")
