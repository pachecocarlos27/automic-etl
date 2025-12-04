"""Logging configuration for Automic ETL.

Provides production-ready structured logging with:
- JSON and console formatters
- Context propagation for tracing
- Pipeline and request correlation
- Performance timing
- Audit logging support
"""

from __future__ import annotations

import logging
import sys
import time
import uuid
from contextvars import ContextVar
from functools import wraps
from pathlib import Path
from typing import Any, Callable, TypeVar

import structlog

# Context variables for request/pipeline tracking
request_id_var: ContextVar[str | None] = ContextVar("request_id", default=None)
pipeline_id_var: ContextVar[str | None] = ContextVar("pipeline_id", default=None)
company_id_var: ContextVar[str | None] = ContextVar("company_id", default=None)
user_id_var: ContextVar[str | None] = ContextVar("user_id", default=None)

F = TypeVar("F", bound=Callable[..., Any])


def add_context_info(
    logger: structlog.BoundLogger,
    method_name: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    """Add context information to log events."""
    # Add request/pipeline context if available
    if request_id := request_id_var.get():
        event_dict["request_id"] = request_id
    if pipeline_id := pipeline_id_var.get():
        event_dict["pipeline_id"] = pipeline_id
    if company_id := company_id_var.get():
        event_dict["company_id"] = company_id
    if user_id := user_id_var.get():
        event_dict["user_id"] = user_id

    return event_dict


def sanitize_event(
    logger: structlog.BoundLogger,
    method_name: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    """Sanitize sensitive data from logs."""
    sensitive_keys = {
        "password", "secret", "token", "api_key", "apikey",
        "authorization", "auth", "credential", "private_key",
        "access_key", "secret_key", "connection_string",
    }

    def sanitize_dict(d: dict) -> dict:
        result = {}
        for key, value in d.items():
            key_lower = key.lower()
            if any(s in key_lower for s in sensitive_keys):
                result[key] = "[REDACTED]"
            elif isinstance(value, dict):
                result[key] = sanitize_dict(value)
            elif isinstance(value, list):
                result[key] = [
                    sanitize_dict(item) if isinstance(item, dict) else item
                    for item in value
                ]
            else:
                result[key] = value
        return result

    return sanitize_dict(event_dict)


def setup_logging(
    level: str = "INFO",
    format: str = "json",
    log_file: str | None = None,
    service_name: str = "automic-etl",
    environment: str = "development",
    sanitize_logs: bool = True,
) -> None:
    """
    Configure structured logging for the application.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format: Output format (json or console)
        log_file: Optional file path for log output
        service_name: Service name for log identification
        environment: Environment name (development, staging, production)
        sanitize_logs: Whether to sanitize sensitive data from logs
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    # Build processor chain
    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        add_context_info,
    ]

    # Add sanitization in production
    if sanitize_logs:
        shared_processors.append(sanitize_event)

    # Add exception formatting
    shared_processors.append(
        structlog.processors.format_exc_info,
    )

    # Format-specific processors
    if format == "json":
        processors = shared_processors + [
            structlog.processors.JSONRenderer(),
        ]
    else:
        processors = shared_processors + [
            structlog.dev.ConsoleRenderer(
                colors=True,
                exception_formatter=structlog.dev.plain_traceback,
            ),
        ]

    # Configure structlog
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
        force=True,
    )

    # Add file handler if specified
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        logging.getLogger().addHandler(file_handler)

    # Log startup
    logger = structlog.get_logger("automic_etl")
    logger.info(
        "Logging initialized",
        service=service_name,
        environment=environment,
        level=level,
        format=format,
    )


def get_logger(name: str | None = None, **initial_context: Any) -> structlog.BoundLogger:
    """
    Get a logger instance with optional initial context.

    Args:
        name: Logger name (module name recommended)
        **initial_context: Initial context to bind to the logger

    Returns:
        Configured structlog BoundLogger
    """
    logger = structlog.get_logger(name)
    if initial_context:
        logger = logger.bind(**initial_context)
    return logger


def set_request_context(
    request_id: str | None = None,
    pipeline_id: str | None = None,
    company_id: str | None = None,
    user_id: str | None = None,
) -> None:
    """
    Set context variables for the current request/pipeline.

    Args:
        request_id: Unique request identifier
        pipeline_id: Pipeline execution identifier
        company_id: Company/tenant identifier
        user_id: User identifier
    """
    if request_id is not None:
        request_id_var.set(request_id)
    if pipeline_id is not None:
        pipeline_id_var.set(pipeline_id)
    if company_id is not None:
        company_id_var.set(company_id)
    if user_id is not None:
        user_id_var.set(user_id)


def clear_request_context() -> None:
    """Clear all request context variables."""
    request_id_var.set(None)
    pipeline_id_var.set(None)
    company_id_var.set(None)
    user_id_var.set(None)


def generate_request_id() -> str:
    """Generate a unique request ID."""
    return str(uuid.uuid4())


def log_execution_time(
    logger: structlog.BoundLogger | None = None,
    level: str = "info",
    message: str = "Operation completed",
) -> Callable[[F], F]:
    """
    Decorator to log function execution time.

    Args:
        logger: Logger to use (creates one if not provided)
        level: Log level for the message
        message: Log message template

    Returns:
        Decorated function
    """
    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            log = logger or get_logger(func.__module__)
            start_time = time.perf_counter()

            try:
                result = func(*args, **kwargs)
                duration = time.perf_counter() - start_time
                getattr(log, level)(
                    message,
                    function=func.__name__,
                    duration_seconds=round(duration, 4),
                    status="success",
                )
                return result
            except Exception as e:
                duration = time.perf_counter() - start_time
                log.error(
                    f"{message} - failed",
                    function=func.__name__,
                    duration_seconds=round(duration, 4),
                    status="error",
                    error=str(e),
                    exc_info=True,
                )
                raise

        return wrapper  # type: ignore

    return decorator


def log_async_execution_time(
    logger: structlog.BoundLogger | None = None,
    level: str = "info",
    message: str = "Async operation completed",
) -> Callable[[F], F]:
    """
    Decorator to log async function execution time.

    Args:
        logger: Logger to use (creates one if not provided)
        level: Log level for the message
        message: Log message template

    Returns:
        Decorated async function
    """
    def decorator(func: F) -> F:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            log = logger or get_logger(func.__module__)
            start_time = time.perf_counter()

            try:
                result = await func(*args, **kwargs)
                duration = time.perf_counter() - start_time
                getattr(log, level)(
                    message,
                    function=func.__name__,
                    duration_seconds=round(duration, 4),
                    status="success",
                )
                return result
            except Exception as e:
                duration = time.perf_counter() - start_time
                log.error(
                    f"{message} - failed",
                    function=func.__name__,
                    duration_seconds=round(duration, 4),
                    status="error",
                    error=str(e),
                    exc_info=True,
                )
                raise

        return wrapper  # type: ignore

    return decorator


class AuditLogger:
    """
    Audit logger for security-sensitive operations.

    Logs important operations with full context for compliance and debugging.
    """

    def __init__(self, service: str = "automic-etl"):
        self.logger = get_logger("audit", service=service, audit=True)

    def log_auth_event(
        self,
        event: str,
        user_id: str | None = None,
        success: bool = True,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Log authentication events."""
        self.logger.info(
            f"auth.{event}",
            user_id=user_id,
            success=success,
            event_type="authentication",
            **(details or {}),
        )

    def log_access_event(
        self,
        resource: str,
        action: str,
        user_id: str | None = None,
        company_id: str | None = None,
        granted: bool = True,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Log access control events."""
        self.logger.info(
            f"access.{action}",
            resource=resource,
            user_id=user_id,
            company_id=company_id,
            granted=granted,
            event_type="access_control",
            **(details or {}),
        )

    def log_data_event(
        self,
        operation: str,
        table: str,
        rows_affected: int | None = None,
        user_id: str | None = None,
        company_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Log data modification events."""
        self.logger.info(
            f"data.{operation}",
            table=table,
            rows_affected=rows_affected,
            user_id=user_id,
            company_id=company_id,
            event_type="data_modification",
            **(details or {}),
        )

    def log_admin_event(
        self,
        action: str,
        admin_user_id: str,
        target_user_id: str | None = None,
        target_company_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Log administrative actions."""
        self.logger.warning(
            f"admin.{action}",
            admin_user_id=admin_user_id,
            target_user_id=target_user_id,
            target_company_id=target_company_id,
            event_type="administrative",
            **(details or {}),
        )


# Module-level audit logger instance
audit_logger = AuditLogger()


# Export for convenience
__all__ = [
    "setup_logging",
    "get_logger",
    "set_request_context",
    "clear_request_context",
    "generate_request_id",
    "log_execution_time",
    "log_async_execution_time",
    "AuditLogger",
    "audit_logger",
    "request_id_var",
    "pipeline_id_var",
    "company_id_var",
    "user_id_var",
]
