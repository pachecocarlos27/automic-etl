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
]
