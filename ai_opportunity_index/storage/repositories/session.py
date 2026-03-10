"""Async SQLAlchemy engine and session factory."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

# Prevent asyncpg from reading a potentially malformed ~/.pgpass file.
# Local dev uses peer/trust auth with no password.
if "PGPASSFILE" not in os.environ:
    os.environ["PGPASSFILE"] = "/dev/null"

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from ai_opportunity_index.config import DATABASE_URL, DB_MAX_OVERFLOW, DB_POOL_SIZE

logger = logging.getLogger(__name__)

_async_engine: AsyncEngine | None = None
_async_session_factory: async_sessionmaker[AsyncSession] | None = None


def _make_async_url(url: str) -> str:
    """Convert a sync postgresql:// URL to async postgresql+asyncpg://"""
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    if url.startswith("postgresql+asyncpg://"):
        return url
    return url


def get_async_engine() -> AsyncEngine:
    global _async_engine
    if _async_engine is None:
        async_url = _make_async_url(DATABASE_URL)
        _async_engine = create_async_engine(
            async_url,
            echo=False,
            pool_size=DB_POOL_SIZE,
            max_overflow=DB_MAX_OVERFLOW,
        )
        logger.info(
            "Async engine created: %s",
            async_url.split("@")[-1] if "@" in async_url else async_url,
        )
    return _async_engine


def get_async_session_factory() -> async_sessionmaker[AsyncSession]:
    global _async_session_factory
    if _async_session_factory is None:
        _async_session_factory = async_sessionmaker(
            bind=get_async_engine(),
            class_=AsyncSession,
            expire_on_commit=False,
        )
    return _async_session_factory


@asynccontextmanager
async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    """Async context manager for database sessions."""
    factory = get_async_session_factory()
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
