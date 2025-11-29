"""Resource management utilities."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Generator

import structlog

logger = structlog.get_logger()


@contextmanager
def safe_resource(
    resource: Any,
    resource_name: str = "resource",
    close_method: str = "close",
    cleanup_on_error: bool = True,
) -> Generator[Any, None, None]:
    """
    Context manager for safe resource cleanup.

    Ensures resources are properly closed even if exceptions occur.

    Args:
        resource: Resource to manage
        resource_name: Name for logging
        close_method: Method name to call for cleanup
        cleanup_on_error: Whether to cleanup on errors

    Yields:
        The resource
    """
    try:
        logger.debug(
            "Resource acquired",
            resource=resource_name,
            resource_type=type(resource).__name__,
        )
        yield resource
    except Exception as e:
        logger.error(
            "Error while using resource",
            resource=resource_name,
            error=str(e),
        )
        if cleanup_on_error:
            try:
                cleanup_method = getattr(resource, close_method, None)
                if callable(cleanup_method):
                    cleanup_method()
                    logger.debug("Resource cleaned up after error", resource=resource_name)
            except Exception as cleanup_error:
                logger.error(
                    "Error during resource cleanup",
                    resource=resource_name,
                    cleanup_error=str(cleanup_error),
                )
        raise
    finally:
        try:
            cleanup_method = getattr(resource, close_method, None)
            if callable(cleanup_method):
                cleanup_method()
                logger.debug("Resource released", resource=resource_name)
        except Exception as e:
            logger.error(
                "Error releasing resource",
                resource=resource_name,
                error=str(e),
            )


@contextmanager
def pooled_connection(
    pool: Any,
    timeout: int = 30,
) -> Generator[Any, None, None]:
    """
    Context manager for database connection pooling.

    Args:
        pool: Connection pool
        timeout: Connection timeout in seconds

    Yields:
        A connection from the pool
    """
    connection = None
    try:
        logger.debug("Acquiring connection from pool", timeout=timeout)
        connection = pool.getconn(timeout=timeout)
        yield connection
    finally:
        if connection is not None:
            try:
                pool.putconn(connection)
                logger.debug("Connection returned to pool")
            except Exception as e:
                logger.error("Error returning connection to pool", error=str(e))


@contextmanager
def temporary_settings(obj: Any, **temporary_values: Any) -> Generator[None, None, None]:
    """
    Temporarily override object attributes.

    Restores original values after context exits.

    Args:
        obj: Object to modify
        **temporary_values: Attribute names and temporary values

    Yields:
        None
    """
    original_values = {}

    try:
        for key, value in temporary_values.items():
            original_values[key] = getattr(obj, key, None)
            setattr(obj, key, value)
            logger.debug("Temporarily set attribute", attr=key, value=value)

        yield

    finally:
        for key, original_value in original_values.items():
            if original_value is None:
                delattr(obj, key) if hasattr(obj, key) else None
            else:
                setattr(obj, key, original_value)
            logger.debug("Restored attribute", attr=key, value=original_value)


class ResourcePool:
    """Simple resource pool for connection management."""

    def __init__(self, factory: Any, max_size: int = 5) -> None:
        """
        Initialize resource pool.

        Args:
            factory: Callable that creates resources
            max_size: Maximum pool size
        """
        self.factory = factory
        self.max_size = max_size
        self.resources: list[Any] = []
        self.logger = logger.bind(component="resource_pool", max_size=max_size)

    def acquire(self, timeout: int = 30) -> Any:
        """
        Acquire a resource from pool.

        Args:
            timeout: Timeout in seconds

        Returns:
            A resource
        """
        if not self.resources and len(self.resources) < self.max_size:
            resource = self.factory()
            self.logger.debug("Created new resource", pool_size=len(self.resources))
            return resource

        if self.resources:
            resource = self.resources.pop()
            self.logger.debug("Reused pooled resource", pool_size=len(self.resources))
            return resource

        self.logger.warning("Pool exhausted, creating new resource")
        return self.factory()

    def release(self, resource: Any) -> None:
        """
        Release a resource back to pool.

        Args:
            resource: Resource to release
        """
        if len(self.resources) < self.max_size:
            self.resources.append(resource)
            self.logger.debug("Resource returned to pool", pool_size=len(self.resources))
        else:
            try:
                if hasattr(resource, "close"):
                    resource.close()
            except Exception as e:
                self.logger.error("Error closing resource", error=str(e))

    @contextmanager
    def get_resource(self) -> Generator[Any, None, None]:
        """
        Context manager for resource acquisition and release.

        Yields:
            A resource
        """
        resource = self.acquire()
        try:
            yield resource
        finally:
            self.release(resource)
