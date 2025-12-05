"""LLM client abstraction for multiple providers."""

from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable
from functools import wraps

import structlog

from automic_etl.core.config import LLMProvider, Settings
from automic_etl.core.exceptions import LLMError
from automic_etl.core.utils import utc_now

logger = structlog.get_logger()


# ============================================================================
# Retry Configuration
# ============================================================================

@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_retries: int = 3
    initial_delay: float = 1.0  # seconds
    max_delay: float = 60.0  # seconds
    exponential_base: float = 2.0
    retryable_errors: tuple = (
        "rate_limit",
        "timeout",
        "server_error",
        "connection_error",
    )


def _classify_error(error: Exception) -> str:
    """Classify an error for retry decisions."""
    error_str = str(error).lower()

    # Rate limiting
    if "rate" in error_str or "429" in error_str or "quota" in error_str:
        return "rate_limit"

    # Timeouts
    if "timeout" in error_str or "timed out" in error_str:
        return "timeout"

    # Server errors
    if any(code in error_str for code in ["500", "502", "503", "504"]):
        return "server_error"

    # Connection errors
    if "connection" in error_str or "network" in error_str:
        return "connection_error"

    # Authentication errors (not retryable)
    if "401" in error_str or "403" in error_str or "auth" in error_str:
        return "auth_error"

    # Invalid request (not retryable)
    if "400" in error_str or "invalid" in error_str:
        return "invalid_request"

    return "unknown"


def with_retry(config: RetryConfig | None = None):
    """
    Decorator for adding retry logic to LLM calls.

    Uses exponential backoff with jitter.
    """
    if config is None:
        config = RetryConfig()

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None

            for attempt in range(config.max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    error_type = _classify_error(e)

                    # Check if error is retryable
                    if error_type not in config.retryable_errors:
                        raise

                    # Check if we have more retries
                    if attempt >= config.max_retries:
                        raise

                    # Calculate delay with exponential backoff and jitter
                    delay = min(
                        config.initial_delay * (config.exponential_base ** attempt),
                        config.max_delay
                    )
                    # Add jitter (+-25%)
                    delay = delay * (0.75 + random.random() * 0.5)

                    logger.warning(
                        "LLM call failed, retrying",
                        attempt=attempt + 1,
                        max_retries=config.max_retries,
                        delay=delay,
                        error_type=error_type,
                        error=str(e),
                    )

                    time.sleep(delay)

            # Should not reach here, but just in case
            if last_error:
                raise last_error

        return wrapper
    return decorator


# ============================================================================
# Rate Limiting
# ============================================================================

@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""
    requests_per_minute: int = 60
    tokens_per_minute: int = 100000
    requests_per_day: int = 10000
    tokens_per_day: int = 1000000


class RateLimiter:
    """
    Rate limiter for LLM API calls.

    Tracks requests and tokens per minute and per day.
    """

    def __init__(self, config: RateLimitConfig | None = None):
        self.config = config or RateLimitConfig()
        self._minute_requests: list[datetime] = []
        self._minute_tokens: list[tuple[datetime, int]] = []
        self._day_requests: list[datetime] = []
        self._day_tokens: list[tuple[datetime, int]] = []

    def check_limit(self, estimated_tokens: int = 0) -> tuple[bool, str | None]:
        """
        Check if a request is within rate limits.

        Returns:
            Tuple of (allowed, reason if not allowed)
        """
        now = utc_now()
        minute_ago = now - timedelta(minutes=1)
        day_ago = now - timedelta(days=1)

        # Clean old entries
        self._minute_requests = [t for t in self._minute_requests if t > minute_ago]
        self._minute_tokens = [(t, n) for t, n in self._minute_tokens if t > minute_ago]
        self._day_requests = [t for t in self._day_requests if t > day_ago]
        self._day_tokens = [(t, n) for t, n in self._day_tokens if t > day_ago]

        # Check minute limits
        if len(self._minute_requests) >= self.config.requests_per_minute:
            return False, f"Rate limit: {self.config.requests_per_minute} requests/min"

        minute_tokens = sum(n for _, n in self._minute_tokens) + estimated_tokens
        if minute_tokens > self.config.tokens_per_minute:
            return False, f"Token limit: {self.config.tokens_per_minute} tokens/min"

        # Check daily limits
        if len(self._day_requests) >= self.config.requests_per_day:
            return False, f"Daily limit: {self.config.requests_per_day} requests/day"

        day_tokens = sum(n for _, n in self._day_tokens) + estimated_tokens
        if day_tokens > self.config.tokens_per_day:
            return False, f"Daily token limit: {self.config.tokens_per_day} tokens/day"

        return True, None

    def record_request(self, tokens_used: int):
        """Record a completed request."""
        now = utc_now()
        self._minute_requests.append(now)
        self._minute_tokens.append((now, tokens_used))
        self._day_requests.append(now)
        self._day_tokens.append((now, tokens_used))

    def get_usage(self) -> dict[str, Any]:
        """Get current usage statistics."""
        now = utc_now()
        minute_ago = now - timedelta(minutes=1)
        day_ago = now - timedelta(days=1)

        minute_requests = len([t for t in self._minute_requests if t > minute_ago])
        minute_tokens = sum(n for t, n in self._minute_tokens if t > minute_ago)
        day_requests = len([t for t in self._day_requests if t > day_ago])
        day_tokens = sum(n for t, n in self._day_tokens if t > day_ago)

        return {
            "minute": {
                "requests": minute_requests,
                "requests_limit": self.config.requests_per_minute,
                "tokens": minute_tokens,
                "tokens_limit": self.config.tokens_per_minute,
            },
            "day": {
                "requests": day_requests,
                "requests_limit": self.config.requests_per_day,
                "tokens": day_tokens,
                "tokens_limit": self.config.tokens_per_day,
            },
        }


def estimate_tokens(text: str) -> int:
    """
    Estimate the number of tokens in a text.

    Uses a simple heuristic: ~4 characters per token.
    """
    return max(1, len(text) // 4)


@dataclass
class LLMResponse:
    """Response from LLM."""

    content: str
    model: str
    tokens_used: int
    finish_reason: str
    metadata: dict[str, Any] = field(default_factory=dict)


class LLMClient:
    """
    Unified LLM client supporting multiple providers.

    Features:
    - Multiple provider support (Anthropic, OpenAI, Ollama, LiteLLM)
    - Automatic retry with exponential backoff
    - Rate limiting with token tracking
    - Usage statistics
    """

    def __init__(
        self,
        settings: Settings,
        retry_config: RetryConfig | None = None,
        rate_limit_config: RateLimitConfig | None = None,
    ) -> None:
        self.settings = settings
        self.llm_config = settings.llm
        self.logger = logger.bind(
            component="llm_client",
            provider=self.llm_config.provider.value,
            model=self.llm_config.model,
        )
        self._client: Any = None
        self._retry_config = retry_config or RetryConfig()
        self._rate_limiter = RateLimiter(rate_limit_config)
        self._total_tokens_used: int = 0
        self._total_requests: int = 0

    def _get_client(self) -> Any:
        """Get or create the LLM client."""
        if self._client is not None:
            return self._client

        provider = self.llm_config.provider

        try:
            if provider == LLMProvider.ANTHROPIC:
                import anthropic
                self._client = anthropic.Anthropic(api_key=self.llm_config.api_key)
            elif provider == LLMProvider.OPENAI:
                import openai
                self._client = openai.OpenAI(api_key=self.llm_config.api_key)
            elif provider == LLMProvider.OLLAMA:
                # Ollama uses OpenAI-compatible API
                import openai
                self._client = openai.OpenAI(
                    api_key="ollama",
                    base_url=self.llm_config.base_url or "http://localhost:11434/v1",
                )
            else:
                # Use LiteLLM for other providers
                import litellm
                self._client = litellm
        except ImportError as e:
            raise LLMError(
                f"Missing dependency for {provider}: {str(e)}",
                provider=provider.value,
            )

        return self._client

    def complete(
        self,
        prompt: str,
        system_prompt: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
        json_mode: bool = False,
    ) -> LLMResponse:
        """
        Generate a completion with retry and rate limiting.

        Args:
            prompt: User prompt
            system_prompt: Optional system prompt
            temperature: Override temperature
            max_tokens: Override max tokens
            json_mode: Request JSON output

        Returns:
            LLMResponse with content and metadata

        Raises:
            LLMError: If rate limited or completion fails after retries
        """
        temperature = temperature or self.llm_config.temperature
        max_tokens = max_tokens or self.llm_config.max_tokens

        # Estimate tokens for rate limiting
        estimated_tokens = estimate_tokens(prompt)
        if system_prompt:
            estimated_tokens += estimate_tokens(system_prompt)

        # Check rate limits
        allowed, reason = self._rate_limiter.check_limit(estimated_tokens)
        if not allowed:
            raise LLMError(
                f"Rate limit exceeded: {reason}",
                provider=self.llm_config.provider.value,
                details={"usage": self._rate_limiter.get_usage()},
            )

        self.logger.debug(
            "Generating completion",
            prompt_length=len(prompt),
            json_mode=json_mode,
            estimated_tokens=estimated_tokens,
        )

        provider = self.llm_config.provider

        # Use retry wrapper for actual completion
        response = self._complete_with_retry(
            prompt, system_prompt, temperature, max_tokens, json_mode, provider
        )

        # Record usage
        self._rate_limiter.record_request(response.tokens_used)
        self._total_tokens_used += response.tokens_used
        self._total_requests += 1

        return response

    def _complete_with_retry(
        self,
        prompt: str,
        system_prompt: str | None,
        temperature: float,
        max_tokens: int,
        json_mode: bool,
        provider: LLMProvider,
    ) -> LLMResponse:
        """Execute completion with retry logic."""
        last_error = None

        for attempt in range(self._retry_config.max_retries + 1):
            try:
                if provider == LLMProvider.ANTHROPIC:
                    return self._complete_anthropic(
                        prompt, system_prompt, temperature, max_tokens
                    )
                elif provider in [LLMProvider.OPENAI, LLMProvider.OLLAMA]:
                    return self._complete_openai(
                        prompt, system_prompt, temperature, max_tokens, json_mode
                    )
                else:
                    return self._complete_litellm(
                        prompt, system_prompt, temperature, max_tokens, json_mode
                    )
            except Exception as e:
                last_error = e
                error_type = _classify_error(e)

                # Check if error is retryable
                if error_type not in self._retry_config.retryable_errors:
                    raise LLMError(
                        f"Completion failed: {str(e)}",
                        provider=provider.value,
                        model=self.llm_config.model,
                    )

                # Check if we have more retries
                if attempt >= self._retry_config.max_retries:
                    raise LLMError(
                        f"Completion failed after {attempt + 1} attempts: {str(e)}",
                        provider=provider.value,
                        model=self.llm_config.model,
                    )

                # Calculate delay with exponential backoff and jitter
                delay = min(
                    self._retry_config.initial_delay * (self._retry_config.exponential_base ** attempt),
                    self._retry_config.max_delay
                )
                delay = delay * (0.75 + random.random() * 0.5)

                self.logger.warning(
                    "LLM call failed, retrying",
                    attempt=attempt + 1,
                    max_retries=self._retry_config.max_retries,
                    delay=delay,
                    error_type=error_type,
                    error=str(e),
                )

                time.sleep(delay)

        # Should not reach here
        if last_error:
            raise LLMError(
                f"Completion failed: {str(last_error)}",
                provider=provider.value,
                model=self.llm_config.model,
            )
        raise LLMError("Unknown error during completion", provider=provider.value)

    def get_usage_stats(self) -> dict[str, Any]:
        """Get usage statistics."""
        return {
            "total_requests": self._total_requests,
            "total_tokens": self._total_tokens_used,
            "rate_limits": self._rate_limiter.get_usage(),
        }

    def _complete_anthropic(
        self,
        prompt: str,
        system_prompt: str | None,
        temperature: float,
        max_tokens: int,
    ) -> LLMResponse:
        """Complete using Anthropic's API."""
        client = self._get_client()

        kwargs: dict[str, Any] = {
            "model": self.llm_config.model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "messages": [{"role": "user", "content": prompt}],
        }

        if system_prompt:
            kwargs["system"] = system_prompt

        response = client.messages.create(**kwargs)

        return LLMResponse(
            content=response.content[0].text,
            model=response.model,
            tokens_used=response.usage.input_tokens + response.usage.output_tokens,
            finish_reason=response.stop_reason,
            metadata={
                "input_tokens": response.usage.input_tokens,
                "output_tokens": response.usage.output_tokens,
            },
        )

    def _complete_openai(
        self,
        prompt: str,
        system_prompt: str | None,
        temperature: float,
        max_tokens: int,
        json_mode: bool,
    ) -> LLMResponse:
        """Complete using OpenAI's API."""
        client = self._get_client()

        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        kwargs: dict[str, Any] = {
            "model": self.llm_config.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }

        if json_mode:
            kwargs["response_format"] = {"type": "json_object"}

        response = client.chat.completions.create(**kwargs)

        choice = response.choices[0]
        return LLMResponse(
            content=choice.message.content or "",
            model=response.model,
            tokens_used=response.usage.total_tokens if response.usage else 0,
            finish_reason=choice.finish_reason or "unknown",
            metadata={
                "input_tokens": response.usage.prompt_tokens if response.usage else 0,
                "output_tokens": response.usage.completion_tokens if response.usage else 0,
            },
        )

    def _complete_litellm(
        self,
        prompt: str,
        system_prompt: str | None,
        temperature: float,
        max_tokens: int,
        json_mode: bool,
    ) -> LLMResponse:
        """Complete using LiteLLM."""
        import litellm

        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        kwargs: dict[str, Any] = {
            "model": self.llm_config.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }

        if self.llm_config.api_key:
            kwargs["api_key"] = self.llm_config.api_key

        if json_mode:
            kwargs["response_format"] = {"type": "json_object"}

        response = litellm.completion(**kwargs)

        choice = response.choices[0]
        return LLMResponse(
            content=choice.message.content or "",
            model=response.model,
            tokens_used=response.usage.total_tokens if response.usage else 0,
            finish_reason=choice.finish_reason or "unknown",
        )

    def complete_json(
        self,
        prompt: str,
        system_prompt: str | None = None,
        schema: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], int]:
        """
        Generate a JSON completion.

        Args:
            prompt: User prompt
            system_prompt: Optional system prompt
            schema: Optional JSON schema for validation

        Returns:
            Tuple of (parsed JSON dict, tokens used)
        """
        # Add JSON instruction to system prompt
        json_instruction = "Respond with valid JSON only. No markdown, no explanations."
        if schema:
            json_instruction += f"\n\nExpected schema: {json.dumps(schema)}"

        full_system = f"{system_prompt}\n\n{json_instruction}" if system_prompt else json_instruction

        response = self.complete(
            prompt=prompt,
            system_prompt=full_system,
            json_mode=True,
        )

        try:
            # Parse JSON from response
            content = response.content.strip()
            # Handle potential markdown code blocks
            if content.startswith("```"):
                content = content.split("```")[1]
                if content.startswith("json"):
                    content = content[4:]
                content = content.strip()

            parsed = json.loads(content)
            return parsed, response.tokens_used
        except json.JSONDecodeError as e:
            raise LLMError(
                f"Failed to parse JSON response: {str(e)}",
                provider=self.llm_config.provider.value,
                details={"content": response.content[:500]},
            )

    def batch_complete(
        self,
        prompts: list[str],
        system_prompt: str | None = None,
        temperature: float | None = None,
    ) -> list[LLMResponse]:
        """
        Complete multiple prompts.

        Currently processes sequentially. Future: support batch APIs.
        """
        responses = []
        for prompt in prompts:
            response = self.complete(
                prompt=prompt,
                system_prompt=system_prompt,
                temperature=temperature,
            )
            responses.append(response)
        return responses
