"""Notification system for Automic ETL."""

from automic_etl.notifications.notifier import (
    Notifier,
    NotificationChannel,
    Notification,
    NotificationLevel,
)
from automic_etl.notifications.channels import (
    EmailNotifier,
    SlackNotifier,
    WebhookNotifier,
    TeamsNotifier,
)
from automic_etl.notifications.alerts import AlertManager, Alert, AlertRule
from automic_etl.notifications.event_service import (
    NotificationEventService,
    EventType,
    get_notification_event_service,
)

__all__ = [
    "Notifier",
    "NotificationChannel",
    "Notification",
    "NotificationLevel",
    "EmailNotifier",
    "SlackNotifier",
    "WebhookNotifier",
    "TeamsNotifier",
    "AlertManager",
    "Alert",
    "AlertRule",
    "NotificationEventService",
    "EventType",
    "get_notification_event_service",
]
