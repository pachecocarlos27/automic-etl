"""Retry utilities with exponential backoff."""

from __future__ import annotations

import time
from typing import Any, Callable, TypeVar

import structlog

from automic_etl.core.exceptions import RetryExhaustedError

logger = structlog.get_logger()

T = TypeVar("T")


def retry_with_backoff(
    func: Callable[..., T],
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    max_delay: float = 60.0,
    jitter: bool = True,
    exception_types: tuple[type[Exception], ...] = (Exception,),
) -> T:
    """
    Execute function with exponential backoff retry logic.

    Args:
        func: Function to execute
        max_attempts: Maximum retry attempts
        initial_delay: Initial delay in seconds
        backoff_factor: Multiplier for delay after each attempt
        max_delay: Maximum delay between retries
        jitter: Whether to add random jitter to delay
        exception_types: Exception types to catch and retry

    Returns:
        Function result

    Raises:
        RetryExhaustedError: If all retry attempts exhausted
    """
    last_error: Exception | None = None
    delay = initial_delay

    for attempt in range(1, max_attempts + 1):
        try:
            logger.debug(
                "Attempting function execution",
                attempt=attempt,
                max_attempts=max_attempts,
            )
            return func()

        except exception_types as e:
            last_error = e
            logger.warning(
                "Function execution failed",
                attempt=attempt,
                max_attempts=max_attempts,
                error=str(e),
            )

            if attempt == max_attempts:
                break

            if jitter:
                import random
                actual_delay = delay * (1 + random.random())
            else:
                actual_delay = delay

            actual_delay = min(actual_delay, max_delay)

            logger.debug(
                "Waiting before retry",
                delay_seconds=actual_delay,
                attempt=attempt,
            )
            time.sleep(actual_delay)
            delay = min(delay * backoff_factor, max_delay)

    raise RetryExhaustedError(
        f"Function failed after {max_attempts} attempts",
        attempts=max_attempts,
        last_error=last_error,
    )


class RetryConfig:
    """Configuration for retry behavior."""

    def __init__(
        self,
        max_attempts: int = 3,
        initial_delay: float = 1.0,
        backoff_factor: float = 2.0,
        max_delay: float = 60.0,
        jitter: bool = True,
        exception_types: tuple[type[Exception], ...] = (Exception,),
    ) -> None:
        """
        Initialize retry configuration.

        Args:
            max_attempts: Maximum retry attempts
            initial_delay: Initial delay in seconds
            backoff_factor: Multiplier for delay after each attempt
            max_delay: Maximum delay between retries
            jitter: Whether to add random jitter
            exception_types: Exception types to retry on
        """
        self.max_attempts = max_attempts
        self.initial_delay = initial_delay
        self.backoff_factor = backoff_factor
        self.max_delay = max_delay
        self.jitter = jitter
        self.exception_types = exception_types

    def execute(self, func: Callable[..., T]) -> T:
        """Execute function with retry logic."""
        return retry_with_backoff(
            func,
            max_attempts=self.max_attempts,
            initial_delay=self.initial_delay,
            backoff_factor=self.backoff_factor,
            max_delay=self.max_delay,
            jitter=self.jitter,
            exception_types=self.exception_types,
        )


class RetryableOperation:
    """Decorator for adding retry logic to functions."""

    def __init__(
        self,
        max_attempts: int = 3,
        initial_delay: float = 1.0,
        backoff_factor: float = 2.0,
    ) -> None:
        """Initialize retryable operation."""
        self.config = RetryConfig(
            max_attempts=max_attempts,
            initial_delay=initial_delay,
            backoff_factor=backoff_factor,
        )

    def __call__(self, func: Callable[..., T]) -> Callable[..., T]:
        """Apply retry decorator."""
        def wrapper(*args: Any, **kwargs: Any) -> T:
            return self.config.execute(lambda: func(*args, **kwargs))

        return wrapper
