"""Unified cache utilities for the pipeline.

Provides TTL checking and version stamping based on per-stage cache policies
defined in config.CACHE_POLICIES.

Existing caches without a ``_cache_version`` key will auto-invalidate
(version mismatch ``None != "v1"``), which is the correct first-run-after-
migration behavior.
"""

import json
import logging
import time
from pathlib import Path

from ai_opportunity_index.config import CACHE_POLICIES

logger = logging.getLogger(__name__)


def cache_is_fresh(path: Path, stage: str, *, force: bool = False) -> bool:
    """Check whether *path* holds a valid, non-expired cache for *stage*.

    Returns ``False`` (= stale) when any of the following is true:
    - *force* is ``True``
    - *path* does not exist
    - The file's ``_cache_version`` key doesn't match the policy version
    - ``check_type="age"`` and the file is older than ``ttl_days``

    ``check_type="none"`` stages always return ``False`` (i.e. the stage
    executor itself decides whether to re-run).
    """
    policy = CACHE_POLICIES.get(stage)
    if policy is None:
        logger.warning("No cache policy for stage %r; treating as stale", stage)
        return False

    if force:
        return False

    if policy.check_type == "none":
        return False

    if not path.exists():
        return False

    # Version check (JSON files only — skip for non-JSON)
    if path.suffix == ".json":
        try:
            data = json.loads(path.read_text())
            file_version = data.get("_cache_version")
            if file_version != policy.cache_version:
                logger.debug(
                    "Cache version mismatch for %s: got %s, want %s",
                    path, file_version, policy.cache_version,
                )
                return False
        except (json.JSONDecodeError, OSError):
            return False

    # Existence-only check — no TTL
    if policy.check_type == "existence":
        return True

    # Age check (check_type == "age")
    if policy.ttl_days is not None:
        age_seconds = time.time() - path.stat().st_mtime
        if age_seconds >= policy.ttl_days * 86400:
            return False

    return True


def stamp_cache(data: dict, stage: str) -> dict:
    """Add ``_cache_version`` to *data* based on the policy for *stage*.

    Returns the same dict (mutated in-place) for convenience.
    """
    policy = CACHE_POLICIES.get(stage)
    if policy is not None:
        data["_cache_version"] = policy.cache_version
    return data
