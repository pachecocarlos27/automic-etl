"""Core notification system."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any
from datetime import datetime
from enum import Enum

import structlog

logger = structlog.get_logger()


class NotificationLevel(Enum):
    """Notification severity levels."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class Notification:
    """A notification message."""
    title: str
    message: str
    level: NotificationLevel = NotificationLevel.INFO
    source: str | None = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    metadata: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "title": self.title,
            "message": self.message,
            "level": self.level.value,
            "source": self.source,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata,
            "tags": self.tags,
        }


class NotificationChannel(ABC):
    """Abstract base class for notification channels."""

    @abstractmethod
    def send(self, notification: Notification) -> bool:
        """Send a notification."""
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        """Test the notification channel."""
        pass


class Notifier:
    """
    Central notification manager.

    Features:
    - Multiple notification channels
    - Filtering by level and tags
    - Rate limiting
    - Notification history
    """

    def __init__(
        self,
        min_level: NotificationLevel = NotificationLevel.INFO,
        rate_limit_per_minute: int | None = None,
    ) -> None:
        """
        Initialize notifier.

        Args:
            min_level: Minimum level to send
            rate_limit_per_minute: Maximum notifications per minute
        """
        self.min_level = min_level
        self.rate_limit = rate_limit_per_minute
        self.channels: dict[str, NotificationChannel] = {}
        self.history: list[Notification] = []
        self._sent_count = 0
        self._last_reset = datetime.utcnow()
        self.logger = logger.bind(component="notifier")

    def add_channel(self, name: str, channel: NotificationChannel) -> None:
        """Add a notification channel."""
        self.channels[name] = channel
        self.logger.info("Channel added", channel=name)

    def remove_channel(self, name: str) -> bool:
        """Remove a notification channel."""
        if name in self.channels:
            del self.channels[name]
            return True
        return False

    def _check_rate_limit(self) -> bool:
        """Check if we're within rate limit."""
        if not self.rate_limit:
            return True

        now = datetime.utcnow()
        if (now - self._last_reset).total_seconds() >= 60:
            self._sent_count = 0
            self._last_reset = now

        return self._sent_count < self.rate_limit

    def _should_send(self, notification: Notification) -> bool:
        """Check if notification should be sent."""
        level_order = list(NotificationLevel)
        return level_order.index(notification.level) >= level_order.index(self.min_level)

    def send(
        self,
        title: str,
        message: str,
        level: NotificationLevel = NotificationLevel.INFO,
        channels: list[str] | None = None,
        **metadata: Any,
    ) -> bool:
        """
        Send a notification.

        Args:
            title: Notification title
            message: Notification message
            level: Severity level
            channels: Specific channels to use (None for all)
            metadata: Additional metadata

        Returns:
            True if notification was sent successfully
        """
        notification = Notification(
            title=title,
            message=message,
            level=level,
            metadata=metadata,
        )

        return self.send_notification(notification, channels)

    def send_notification(
        self,
        notification: Notification,
        channels: list[str] | None = None,
    ) -> bool:
        """Send a notification object."""
        if not self._should_send(notification):
            return False

        if not self._check_rate_limit():
            self.logger.warning("Rate limit exceeded")
            return False

        self.history.append(notification)

        target_channels = channels or list(self.channels.keys())
        success = False

        for channel_name in target_channels:
            channel = self.channels.get(channel_name)
            if channel:
                try:
                    if channel.send(notification):
                        success = True
                        self._sent_count += 1
                        self.logger.debug(
                            "Notification sent",
                            channel=channel_name,
                            level=notification.level.value,
                        )
                except Exception as e:
                    self.logger.error(
                        "Failed to send notification",
                        channel=channel_name,
                        error=str(e),
                    )

        return success

    def info(self, title: str, message: str, **metadata: Any) -> bool:
        """Send an info notification."""
        return self.send(title, message, NotificationLevel.INFO, **metadata)

    def warning(self, title: str, message: str, **metadata: Any) -> bool:
        """Send a warning notification."""
        return self.send(title, message, NotificationLevel.WARNING, **metadata)

    def error(self, title: str, message: str, **metadata: Any) -> bool:
        """Send an error notification."""
        return self.send(title, message, NotificationLevel.ERROR, **metadata)

    def critical(self, title: str, message: str, **metadata: Any) -> bool:
        """Send a critical notification."""
        return self.send(title, message, NotificationLevel.CRITICAL, **metadata)

    # Pre-built notification templates
    def pipeline_started(self, pipeline_name: str) -> bool:
        """Notify pipeline start."""
        return self.info(
            title=f"Pipeline Started: {pipeline_name}",
            message=f"Pipeline '{pipeline_name}' has started execution.",
            pipeline=pipeline_name,
        )

    def pipeline_completed(
        self,
        pipeline_name: str,
        duration_seconds: float,
        rows_processed: int | None = None,
    ) -> bool:
        """Notify pipeline completion."""
        message = f"Pipeline '{pipeline_name}' completed in {duration_seconds:.1f}s"
        if rows_processed:
            message += f", processed {rows_processed:,} rows"

        return self.info(
            title=f"Pipeline Completed: {pipeline_name}",
            message=message,
            pipeline=pipeline_name,
            duration=duration_seconds,
            rows=rows_processed,
        )

    def pipeline_failed(
        self,
        pipeline_name: str,
        error: str,
        step: str | None = None,
    ) -> bool:
        """Notify pipeline failure."""
        message = f"Pipeline '{pipeline_name}' failed"
        if step:
            message += f" at step '{step}'"
        message += f": {error}"

        return self.error(
            title=f"Pipeline Failed: {pipeline_name}",
            message=message,
            pipeline=pipeline_name,
            error=error,
            step=step,
        )

    def data_quality_alert(
        self,
        table_name: str,
        issues: list[str],
        severity: NotificationLevel = NotificationLevel.WARNING,
    ) -> bool:
        """Notify data quality issues."""
        return self.send(
            title=f"Data Quality Alert: {table_name}",
            message=f"Data quality issues found:\n" + "\n".join(f"- {i}" for i in issues),
            level=severity,
            table=table_name,
            issues=issues,
        )

    def job_alert(
        self,
        job_name: str,
        status: str,
        message: str,
        level: NotificationLevel = NotificationLevel.INFO,
    ) -> bool:
        """Generic job alert."""
        return self.send(
            title=f"Job Alert: {job_name}",
            message=f"Job '{job_name}' status: {status}\n{message}",
            level=level,
            job=job_name,
            status=status,
        )

    def get_history(
        self,
        level: NotificationLevel | None = None,
        limit: int = 100,
    ) -> list[Notification]:
        """Get notification history."""
        history = self.history[-limit:]

        if level:
            history = [n for n in history if n.level == level]

        return history
