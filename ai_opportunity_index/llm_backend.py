"""Centralized LLM backend: switch between Google Vertex, Groq, and Claude Code CLI.

Usage:
    from ai_opportunity_index.llm_backend import get_agent

    agent = get_agent(output_type=MyModel, system_prompt="You are...")
    result = agent.run_sync("prompt")       # sync
    result = await agent.run("prompt")      # async
    print(result.output)                    # parsed Pydantic model

Config via env var:
    LLM_BACKEND=google       (default) — Gemini via Vertex AI + pydantic-ai
    LLM_BACKEND=groq         — Groq API (free tier, OpenAI-compatible JSON mode)
    LLM_BACKEND=claude-code  — Claude Code CLI (uses your subscription)

Model selection:
    LLM_EXTRACTION_MODEL / LLM_ESTIMATION_MODEL in config.py control model names.
    For Groq, models are set via GROQ_EXTRACTION_MODEL / GROQ_ESTIMATION_MODEL.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import subprocess
from dataclasses import dataclass, field
from typing import Any, Type, TypeVar

from pydantic import BaseModel

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)

LLM_BACKEND = os.environ.get("LLM_BACKEND", "groq")
CLAUDE_CLI_PATH = os.environ.get("CLAUDE_CLI_PATH", "claude")
CLAUDE_CLI_MODEL = os.environ.get("CLAUDE_CLI_MODEL", "")  # empty = default model


# ── Result wrapper ────────────────────────────────────────────────────

@dataclass
class AgentUsage:
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0


@dataclass
class AgentResult:
    """Mimics pydantic-ai RunResult interface."""
    output: Any = None
    _usage: AgentUsage = field(default_factory=AgentUsage)

    def usage(self) -> AgentUsage:
        return self._usage


# ── Groq Agent (OpenAI-compatible JSON mode) ──────────────────────────

# Rate limiter: Groq free tier = 30 req/min. Keep ~2s between calls.
_groq_lock = asyncio.Lock()
_groq_last_call = 0.0
GROQ_MIN_INTERVAL = 0.2  # seconds between calls (paid tier: much higher limits)

_groq_client = None


def _get_groq_client():
    """Lazy-init OpenAI client pointing at Groq."""
    global _groq_client
    if _groq_client is None:
        from openai import OpenAI
        from ai_opportunity_index.config import GROQ_API_KEY
        if not GROQ_API_KEY:
            raise ValueError("GROQ_API_KEY not set. Set it in env or config.py.")
        _groq_client = OpenAI(
            base_url="https://api.groq.com/openai/v1",
            api_key=GROQ_API_KEY,
        )
    return _groq_client


_groq_async_client = None


def _get_groq_async_client():
    """Lazy-init async OpenAI client pointing at Groq."""
    global _groq_async_client
    if _groq_async_client is None:
        from openai import AsyncOpenAI
        from ai_opportunity_index.config import GROQ_API_KEY
        if not GROQ_API_KEY:
            raise ValueError("GROQ_API_KEY not set. Set it in env or config.py.")
        _groq_async_client = AsyncOpenAI(
            base_url="https://api.groq.com/openai/v1",
            api_key=GROQ_API_KEY,
        )
    return _groq_async_client


class GroqAgent:
    """Drop-in replacement for pydantic-ai Agent using Groq's OpenAI-compatible API.

    Uses JSON mode (response_format=json_object) instead of tool calling,
    since Llama/Kimi models on Groq handle JSON mode more reliably.
    """

    def __init__(
        self,
        output_type: Type[T],
        system_prompt: str = "",
        model_name: str = "",
    ):
        self.output_type = output_type
        self.system_prompt = system_prompt
        self.model_name = model_name

    def _build_messages(self, user_prompt: str) -> list[dict]:
        schema = self.output_type.model_json_schema()
        schema_str = json.dumps(schema, indent=2)

        messages = []
        if self.system_prompt:
            messages.append({"role": "system", "content": self.system_prompt})

        messages.append({
            "role": "user",
            "content": (
                f"{user_prompt}\n\n"
                f"## Output Format\n"
                f"Return ONLY a valid JSON object matching this schema:\n"
                f"```json\n{schema_str}\n```\n"
                f"Return raw JSON only — no markdown fences, no explanation, no commentary."
            ),
        })
        return messages

    def _parse_and_validate(self, content: str) -> T:
        """Parse JSON response and validate against output_type."""
        cleaned = content.strip()
        # Strip markdown fences if present
        if cleaned.startswith("```"):
            cleaned = re.sub(r"^```\w*\n?", "", cleaned)
            cleaned = re.sub(r"\n?```\s*$", "", cleaned)
            cleaned = cleaned.strip()
        data = json.loads(cleaned)
        return self.output_type.model_validate(data)

    def run_sync(self, prompt: str) -> AgentResult:
        """Synchronous run via Groq API."""
        client = _get_groq_client()
        messages = self._build_messages(prompt)

        resp = client.chat.completions.create(
            model=self.model_name,
            messages=messages,
            response_format={"type": "json_object"},
            temperature=0,
        )

        content = resp.choices[0].message.content
        output = self._parse_and_validate(content)

        return AgentResult(
            output=output,
            _usage=AgentUsage(
                input_tokens=resp.usage.prompt_tokens or 0,
                output_tokens=resp.usage.completion_tokens or 0,
                total_tokens=resp.usage.total_tokens or 0,
            ),
        )

    async def run(self, prompt: str) -> AgentResult:
        """Async run via Groq API with rate limiting."""
        global _groq_last_call

        # Rate limit: wait if needed
        async with _groq_lock:
            now = asyncio.get_event_loop().time()
            wait = GROQ_MIN_INTERVAL - (now - _groq_last_call)
            if wait > 0:
                await asyncio.sleep(wait)
            _groq_last_call = asyncio.get_event_loop().time()

        client = _get_groq_async_client()
        messages = self._build_messages(prompt)

        resp = await client.chat.completions.create(
            model=self.model_name,
            messages=messages,
            response_format={"type": "json_object"},
            temperature=0,
        )

        content = resp.choices[0].message.content
        output = self._parse_and_validate(content)

        return AgentResult(
            output=output,
            _usage=AgentUsage(
                input_tokens=resp.usage.prompt_tokens or 0,
                output_tokens=resp.usage.completion_tokens or 0,
                total_tokens=resp.usage.total_tokens or 0,
            ),
        )


# ── Claude Code CLI Agent ─────────────────────────────────────────────

class ClaudeCodeAgent:
    """Drop-in replacement for pydantic-ai Agent that uses the `claude` CLI.

    Sends the prompt via stdin to `claude -p`, parses JSON from the response,
    and validates it against the output_type Pydantic model.
    """

    def __init__(
        self,
        output_type: Type[T],
        system_prompt: str = "",
    ):
        self.output_type = output_type
        self.system_prompt = system_prompt

    def _build_prompt(self, user_prompt: str) -> str:
        """Build the full prompt with schema instructions."""
        schema = self.output_type.model_json_schema()
        schema_str = json.dumps(schema, indent=2)

        parts = []
        if self.system_prompt:
            parts.append(self.system_prompt)

        parts.append(user_prompt)

        parts.append(
            f"\n\n## Output Format\n"
            f"Return ONLY a valid JSON object matching this schema:\n"
            f"```json\n{schema_str}\n```\n"
            f"Return raw JSON only — no markdown fences, no explanation, no commentary."
        )
        return "\n\n".join(parts)

    def _parse_response(self, text: str) -> T:
        """Extract JSON from response and validate against output_type."""
        # Try direct parse first
        cleaned = text.strip()

        # Strip markdown code fences if present
        if cleaned.startswith("```"):
            # Remove opening fence (```json or ```)
            cleaned = re.sub(r"^```\w*\n?", "", cleaned)
            cleaned = re.sub(r"\n?```\s*$", "", cleaned)
            cleaned = cleaned.strip()

        # Try to find JSON object/array in the response
        if not cleaned.startswith(("{", "[")):
            # Look for first { or [
            for i, ch in enumerate(cleaned):
                if ch in "{[":
                    cleaned = cleaned[i:]
                    break

        try:
            data = json.loads(cleaned)
        except json.JSONDecodeError:
            # Last resort: find the largest JSON-like block
            matches = re.findall(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', text, re.DOTALL)
            if matches:
                # Try the longest match
                matches.sort(key=len, reverse=True)
                data = json.loads(matches[0])
            else:
                raise ValueError(f"Could not parse JSON from Claude CLI response:\n{text[:500]}")

        return self.output_type.model_validate(data)

    def _call_cli(self, prompt: str) -> str:
        """Call the claude CLI and return the response text."""
        cmd = [CLAUDE_CLI_PATH, "-p"]
        if CLAUDE_CLI_MODEL:
            cmd.extend(["--model", CLAUDE_CLI_MODEL])

        env = os.environ.copy()
        # Remove nested session protection vars
        env.pop("CLAUDECODE", None)
        env.pop("CLAUDE_CODE_ENTRYPOINT", None)

        logger.debug("Calling Claude CLI: %s", " ".join(cmd))
        result = subprocess.run(
            cmd,
            input=prompt,
            capture_output=True,
            text=True,
            timeout=300,  # 5 min timeout
            env=env,
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"Claude CLI failed (exit {result.returncode}): {result.stderr[:500]}"
            )

        return result.stdout

    def run_sync(self, prompt: str) -> AgentResult:
        """Synchronous run — calls claude CLI."""
        full_prompt = self._build_prompt(prompt)
        response_text = self._call_cli(full_prompt)

        output = self._parse_response(response_text)
        # Rough token estimate (4 chars per token)
        input_tokens = len(full_prompt) // 4
        output_tokens = len(response_text) // 4

        return AgentResult(
            output=output,
            _usage=AgentUsage(
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                total_tokens=input_tokens + output_tokens,
            ),
        )

    async def run(self, prompt: str) -> AgentResult:
        """Async run — wraps sync call in a thread."""
        return await asyncio.to_thread(self.run_sync, prompt)


# ── Factory ───────────────────────────────────────────────────────────

def get_agent(
    output_type: Type[T],
    system_prompt: str = "",
    model_name: str | None = None,
) -> Any:
    """Create an LLM agent using the configured backend.

    Args:
        output_type: Pydantic model for structured output.
        system_prompt: Optional system prompt.
        model_name: Override model name. For Groq, defaults to GROQ_EXTRACTION_MODEL.
                     For Google, defaults to LLM_EXTRACTION_MODEL.

    Returns:
        Agent-like object with .run_sync(prompt) and .run(prompt) methods.
    """
    backend = LLM_BACKEND.lower()

    if backend == "claude-code":
        logger.info("Using Claude Code CLI backend")
        return ClaudeCodeAgent(
            output_type=output_type,
            system_prompt=system_prompt,
        )

    if backend == "groq":
        from ai_opportunity_index.config import (
            GROQ_EXTRACTION_MODEL,
            GROQ_ESTIMATION_MODEL,
            LLM_ESTIMATION_MODEL,
        )
        # Map Gemini model names to Groq equivalents
        if model_name == LLM_ESTIMATION_MODEL:
            name = GROQ_ESTIMATION_MODEL
        else:
            name = model_name or GROQ_EXTRACTION_MODEL
        logger.debug("Using Groq backend: %s", name)
        return GroqAgent(
            output_type=output_type,
            system_prompt=system_prompt,
            model_name=name,
        )

    # Default: Google Vertex via pydantic-ai
    from pydantic_ai import Agent
    from pydantic_ai.models.google import GoogleModel

    from ai_opportunity_index.config import (
        LLM_EXTRACTION_MODEL,
        get_google_provider,
    )

    name = model_name or LLM_EXTRACTION_MODEL
    model = GoogleModel(name, provider=get_google_provider())

    kwargs: dict[str, Any] = {"output_type": output_type}
    if system_prompt:
        kwargs["system_prompt"] = system_prompt

    return Agent(model, **kwargs)


# ── Shared retry utilities for LLM calls ──────────────────────────────

def is_retryable_llm_error(exc: BaseException) -> bool:
    """Return True for rate-limit, transient server errors, and event-loop binding issues."""
    s = str(exc)
    return (
        "429" in s
        or "RESOURCE_EXHAUSTED" in s
        or "503" in s
        or "500" in s
        or "event loop" in s.lower()
    )


def make_llm_retry(
    max_attempts: int = 5,
    initial_wait: float = 2,
    max_wait: float = 45,
    jitter: float = 3,
):
    """Create a tenacity retry decorator for LLM calls.

    Usage:
        @make_llm_retry()
        async def my_llm_call():
            ...
    """
    from tenacity import (
        before_sleep_log,
        retry,
        retry_if_exception,
        stop_after_attempt,
        wait_exponential_jitter,
    )

    return retry(
        retry=retry_if_exception(is_retryable_llm_error),
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential_jitter(initial=initial_wait, max=max_wait, jitter=jitter),
        before_sleep=before_sleep_log(logger, logging.INFO),
        reraise=True,
    )


async def run_agent_with_retry(agent, prompt, max_attempts: int = 5):
    """Run an LLM agent with exponential backoff on 429/5xx errors.

    This is the shared retry wrapper for all LLM agent calls.
    """
    _retry = make_llm_retry(max_attempts=max_attempts)

    @_retry
    async def _call():
        return await agent.run(prompt)

    return await _call()
