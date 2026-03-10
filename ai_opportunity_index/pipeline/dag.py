"""DAG definition and topological utilities for the pipeline.

Moved from scripts/run_pipeline.py to enable shared access across all
entry points (CLI, daily refresh, web UI, refresh requests).
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass


# ── DAG Definition ────────────────────────────────────────────────────────

DAG: dict[str, set[str]] = {
    "discover_links": set(),
    "collect_news": set(),
    "collect_github": set(),
    "collect_analysts": set(),
    "collect_web_enrichment": {"discover_links"},
    "extract_filings": set(),
    "extract_news": {"collect_news"},
    "extract_unified": {"collect_news", "collect_github", "collect_analysts", "collect_web_enrichment"},
    "value_evidence": {"extract_news", "extract_filings", "extract_unified"},
    "score": {"value_evidence", "collect_web_enrichment", "collect_github", "collect_analysts"},
}

# Stage aliases for CLI convenience
STAGE_ALIASES: dict[str, set[str]] = {
    "all": set(DAG.keys()),
    "collect": {"discover_links", "collect_news", "collect_github",
                "collect_analysts", "collect_web_enrichment"},
    "extract": {"extract_filings", "extract_news", "extract_unified"},
    "value": {"value_evidence"},
}


def topological_layers(dag: dict[str, set[str]]) -> list[list[str]]:
    """Compute topological layers using Kahn's algorithm.

    Returns layers where all stages in a layer can run concurrently.
    Raises ValueError if the DAG contains a cycle.
    """
    in_degree = {node: len(deps) for node, deps in dag.items()}
    dependents: dict[str, list[str]] = {node: [] for node in dag}
    for node, deps in dag.items():
        for dep in deps:
            dependents[dep].append(node)

    queue = deque(node for node, deg in in_degree.items() if deg == 0)
    layers: list[list[str]] = []
    visited = 0

    while queue:
        layer = []
        for _ in range(len(queue)):
            node = queue.popleft()
            layer.append(node)
            visited += 1
            for dependent in dependents[node]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)
        layers.append(sorted(layer))

    if visited != len(dag):
        raise ValueError("DAG contains a cycle")

    return layers


def resolve_stages(requested: list[str], *, with_deps: bool = False) -> set[str]:
    """Expand aliases and optionally add transitive dependencies.

    By default only alias expansion is performed. Pass ``with_deps=True``
    to pull in all transitive DAG dependencies (old behaviour).
    """
    stages: set[str] = set()
    for s in requested:
        if s in STAGE_ALIASES:
            stages |= STAGE_ALIASES[s]
        elif s in DAG:
            stages.add(s)
        else:
            raise ValueError(f"Unknown stage: {s}")

    if with_deps:
        added = True
        while added:
            added = False
            for stage in list(stages):
                for dep in DAG.get(stage, set()):
                    if dep not in stages:
                        stages.add(dep)
                        added = True

    return stages


def parse_force_stages(force_arg: list[str] | None) -> set[str]:
    """Parse the ``--force`` CLI value into a set of stage names.

    * ``None``  -> ``--force`` was not passed at all -> empty set
    * ``[]``    -> bare ``--force`` with no arguments -> ALL stages
    * ``["extract", "value"]`` -> expand aliases, return matching stages
    """
    if force_arg is None:
        return set()
    if len(force_arg) == 0:
        return set(DAG.keys())
    # Expand aliases
    stages: set[str] = set()
    for s in force_arg:
        if s in STAGE_ALIASES:
            stages |= STAGE_ALIASES[s]
        elif s in DAG:
            stages.add(s)
        else:
            raise ValueError(f"Unknown force stage: {s}")
    return stages


# ── Stage Result ──────────────────────────────────────────────────────────


@dataclass
class StageResult:
    stage: str
    ticker: str
    success: bool
    error: str | None = None
    skipped: bool = False
