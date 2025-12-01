"""Event-based notification service that connects to database alert rules."""

from __future__ import annotations

from datetime import datetime
from typing import Optional, List, Any
from enum import Enum

import structlog

from automic_etl.notifications.notifier import Notifier, NotificationLevel, Notification
from automic_etl.notifications.channels import (
    EmailNotifier,
    SlackNotifier,
    WebhookNotifier,
    TeamsNotifier,
    ConsoleNotifier,
)
from automic_etl.db.alert_service import get_alert_service
from automic_etl.db.models import NotificationChannelModel, AlertRuleModel

logger = structlog.get_logger()


class EventType(Enum):
    """Types of events that can trigger notifications."""
    PIPELINE_STARTED = "pipeline_started"
    PIPELINE_COMPLETED = "pipeline_completed"
    PIPELINE_FAILED = "pipeline_failed"
    JOB_STARTED = "job_started"
    JOB_COMPLETED = "job_completed"
    JOB_FAILED = "job_failed"
    VALIDATION_PASSED = "validation_passed"
    VALIDATION_FAILED = "validation_failed"
    DATA_QUALITY_ALERT = "data_quality_alert"
    CONNECTOR_CONNECTED = "connector_connected"
    CONNECTOR_FAILED = "connector_failed"
    USER_LOGIN = "user_login"
    USER_FAILED_LOGIN = "user_failed_login"
    SYSTEM_ERROR = "system_error"
    CUSTOM = "custom"


class NotificationEventService:
    """
    Service that connects events to the notification system.

    Features:
    - Loads notification channels from database
    - Checks alert rules for event matching
    - Sends notifications through configured channels
    - Records alerts in history
    """

    def __init__(self) -> None:
        self.notifier = Notifier()
        self.alert_service = get_alert_service()
        self.logger = logger.bind(component="notification_event_service")
        self._channels_loaded = False

    def _load_channels(self) -> None:
        """Load notification channels from database."""
        if self._channels_loaded:
            return

        channels = self.alert_service.list_channels(enabled=True)

        for channel in channels:
            notifier_channel = self._create_channel(channel)
            if notifier_channel:
                self.notifier.add_channel(channel.id, notifier_channel)

        # Always add console for development
        self.notifier.add_channel("console", ConsoleNotifier())

        self._channels_loaded = True
        self.logger.info("Loaded notification channels", count=len(channels))

    def _create_channel(self, channel_model: NotificationChannelModel):
        """Create a notification channel from database model."""
        config = channel_model.config or {}
        channel_type = channel_model.channel_type

        try:
            if channel_type == "email":
                return EmailNotifier(
                    smtp_host=config.get("smtp_host", "localhost"),
                    smtp_port=config.get("smtp_port", 587),
                    username=config.get("username"),
                    password=config.get("password"),
                    from_address=config.get("from_address", "automic-etl@localhost"),
                    to_addresses=config.get("to_addresses", []),
                    use_tls=config.get("use_tls", True),
                )

            elif channel_type == "slack":
                return SlackNotifier(
                    webhook_url=config.get("webhook_url", ""),
                    channel=config.get("channel"),
                    username=config.get("username", "Automic ETL"),
                )

            elif channel_type == "webhook":
                return WebhookNotifier(
                    url=config.get("url", ""),
                    method=config.get("method", "POST"),
                    headers=config.get("headers", {}),
                )

            elif channel_type == "teams":
                return TeamsNotifier(
                    webhook_url=config.get("webhook_url", ""),
                )

            elif channel_type == "pagerduty":
                # PagerDuty uses webhook
                return WebhookNotifier(
                    url=f"https://events.pagerduty.com/v2/enqueue",
                    method="POST",
                    headers={"Content-Type": "application/json"},
                )

            else:
                self.logger.warning(f"Unknown channel type: {channel_type}")
                return None

        except Exception as e:
            self.logger.error(f"Failed to create channel", type=channel_type, error=str(e))
            return None

    def reload_channels(self) -> None:
        """Force reload of notification channels."""
        self._channels_loaded = False
        self.notifier.channels.clear()
        self._load_channels()

    def _get_matching_rules(self, event_type: EventType) -> List[AlertRuleModel]:
        """Get alert rules that match the event type."""
        rules = self.alert_service.get_rules_by_type(event_type.value)
        return [r for r in rules if self.alert_service.can_trigger_alert(r.id)]

    def _severity_to_level(self, severity: str) -> NotificationLevel:
        """Convert severity string to NotificationLevel."""
        mapping = {
            "critical": NotificationLevel.CRITICAL,
            "high": NotificationLevel.ERROR,
            "medium": NotificationLevel.WARNING,
            "low": NotificationLevel.INFO,
            "info": NotificationLevel.INFO,
            "warning": NotificationLevel.WARNING,
            "error": NotificationLevel.ERROR,
        }
        return mapping.get(severity.lower(), NotificationLevel.INFO)

    def emit_event(
        self,
        event_type: EventType,
        title: str,
        message: str,
        severity: str = "info",
        source: Optional[str] = None,
        details: Optional[dict] = None,
    ) -> bool:
        """
        Emit an event that may trigger notifications.

        Args:
            event_type: Type of event
            title: Event title
            message: Event message
            severity: Event severity (critical, high, medium, low)
            source: Source of the event (pipeline name, job name, etc.)
            details: Additional event details

        Returns:
            True if notification was sent
        """
        self._load_channels()

        # Get matching alert rules
        rules = self._get_matching_rules(event_type)

        if not rules:
            # No rules match, but log the event
            self.logger.debug("No alert rules for event", event_type=event_type.value)
            return False

        sent = False
        for rule in rules:
            # Check if rule condition matches (if any)
            if rule.condition:
                if not self._evaluate_condition(rule.condition, details or {}):
                    continue

            # Create alert history record
            alert = self.alert_service.create_alert(
                title=title,
                message=message,
                severity=severity,
                rule_id=rule.id,
                source=source,
                details=details,
            )

            # Mark rule as triggered
            self.alert_service.mark_rule_triggered(rule.id)

            # Send notifications through configured channels
            channel_ids = rule.channels or []
            level = self._severity_to_level(rule.severity)

            notification = Notification(
                title=title,
                message=message,
                level=level,
                source=source,
                metadata=details or {},
            )

            for channel_id in channel_ids:
                if channel_id in self.notifier.channels:
                    try:
                        success = self.notifier.channels[channel_id].send(notification)
                        self.alert_service.add_notification_sent(
                            alert.id, channel_id, "configured", success
                        )
                        if success:
                            sent = True
                            # Mark channel as used
                            self.alert_service.mark_channel_used(channel_id, True)
                    except Exception as e:
                        self.logger.error("Failed to send notification", channel=channel_id, error=str(e))
                        self.alert_service.add_notification_sent(
                            alert.id, channel_id, "configured", False
                        )
                        self.alert_service.mark_channel_used(channel_id, False)

        return sent

    def _evaluate_condition(self, condition: dict, details: dict) -> bool:
        """Evaluate alert rule condition against event details."""
        # Simple condition evaluation
        # Supports: equals, contains, greater_than, less_than
        for key, check in condition.items():
            if key not in details:
                continue

            value = details[key]

            if isinstance(check, dict):
                if "equals" in check and value != check["equals"]:
                    return False
                if "contains" in check and check["contains"] not in str(value):
                    return False
                if "greater_than" in check and value <= check["greater_than"]:
                    return False
                if "less_than" in check and value >= check["less_than"]:
                    return False
            elif value != check:
                return False

        return True

    # Convenience methods for common events

    def pipeline_started(self, pipeline_name: str, pipeline_id: str) -> bool:
        """Emit pipeline started event."""
        return self.emit_event(
            EventType.PIPELINE_STARTED,
            title=f"Pipeline Started: {pipeline_name}",
            message=f"Pipeline '{pipeline_name}' has started execution.",
            severity="info",
            source=pipeline_name,
            details={"pipeline_id": pipeline_id, "pipeline_name": pipeline_name},
        )

    def pipeline_completed(
        self,
        pipeline_name: str,
        pipeline_id: str,
        duration_seconds: float,
        rows_processed: int = 0,
    ) -> bool:
        """Emit pipeline completed event."""
        return self.emit_event(
            EventType.PIPELINE_COMPLETED,
            title=f"Pipeline Completed: {pipeline_name}",
            message=f"Pipeline '{pipeline_name}' completed in {duration_seconds:.1f}s, processed {rows_processed:,} rows.",
            severity="info",
            source=pipeline_name,
            details={
                "pipeline_id": pipeline_id,
                "pipeline_name": pipeline_name,
                "duration_seconds": duration_seconds,
                "rows_processed": rows_processed,
            },
        )

    def pipeline_failed(
        self,
        pipeline_name: str,
        pipeline_id: str,
        error: str,
        step: Optional[str] = None,
    ) -> bool:
        """Emit pipeline failed event."""
        message = f"Pipeline '{pipeline_name}' failed"
        if step:
            message += f" at step '{step}'"
        message += f": {error}"

        return self.emit_event(
            EventType.PIPELINE_FAILED,
            title=f"Pipeline Failed: {pipeline_name}",
            message=message,
            severity="critical",
            source=pipeline_name,
            details={
                "pipeline_id": pipeline_id,
                "pipeline_name": pipeline_name,
                "error": error,
                "step": step,
            },
        )

    def job_started(self, job_name: str, job_id: str) -> bool:
        """Emit job started event."""
        return self.emit_event(
            EventType.JOB_STARTED,
            title=f"Job Started: {job_name}",
            message=f"Scheduled job '{job_name}' has started.",
            severity="info",
            source=job_name,
            details={"job_id": job_id, "job_name": job_name},
        )

    def job_completed(
        self,
        job_name: str,
        job_id: str,
        duration_seconds: float,
    ) -> bool:
        """Emit job completed event."""
        return self.emit_event(
            EventType.JOB_COMPLETED,
            title=f"Job Completed: {job_name}",
            message=f"Scheduled job '{job_name}' completed in {duration_seconds:.1f}s.",
            severity="info",
            source=job_name,
            details={
                "job_id": job_id,
                "job_name": job_name,
                "duration_seconds": duration_seconds,
            },
        )

    def job_failed(
        self,
        job_name: str,
        job_id: str,
        error: str,
    ) -> bool:
        """Emit job failed event."""
        return self.emit_event(
            EventType.JOB_FAILED,
            title=f"Job Failed: {job_name}",
            message=f"Scheduled job '{job_name}' failed: {error}",
            severity="high",
            source=job_name,
            details={
                "job_id": job_id,
                "job_name": job_name,
                "error": error,
            },
        )

    def validation_failed(
        self,
        table_name: str,
        rule_name: str,
        failed_rows: int,
        total_rows: int,
    ) -> bool:
        """Emit validation failed event."""
        return self.emit_event(
            EventType.VALIDATION_FAILED,
            title=f"Validation Failed: {rule_name}",
            message=f"Validation rule '{rule_name}' failed on table '{table_name}': {failed_rows:,}/{total_rows:,} rows failed.",
            severity="high",
            source=table_name,
            details={
                "table_name": table_name,
                "rule_name": rule_name,
                "failed_rows": failed_rows,
                "total_rows": total_rows,
                "failure_rate": failed_rows / total_rows if total_rows > 0 else 0,
            },
        )

    def data_quality_alert(
        self,
        table_name: str,
        issues: List[str],
        severity: str = "warning",
    ) -> bool:
        """Emit data quality alert."""
        return self.emit_event(
            EventType.DATA_QUALITY_ALERT,
            title=f"Data Quality Alert: {table_name}",
            message=f"Data quality issues found in '{table_name}':\n" + "\n".join(f"- {i}" for i in issues),
            severity=severity,
            source=table_name,
            details={
                "table_name": table_name,
                "issues": issues,
                "issue_count": len(issues),
            },
        )

    def system_error(self, component: str, error: str) -> bool:
        """Emit system error event."""
        return self.emit_event(
            EventType.SYSTEM_ERROR,
            title=f"System Error: {component}",
            message=f"A system error occurred in '{component}': {error}",
            severity="critical",
            source=component,
            details={"component": component, "error": error},
        )


# Singleton instance
_notification_event_service: Optional[NotificationEventService] = None


def get_notification_event_service() -> NotificationEventService:
    """Get the notification event service singleton."""
    global _notification_event_service
    if _notification_event_service is None:
        _notification_event_service = NotificationEventService()
    return _notification_event_service
