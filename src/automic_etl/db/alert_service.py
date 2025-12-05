"""Database-backed alert and notification service."""

from __future__ import annotations

from datetime import timedelta
from typing import Optional, List
import uuid

from automic_etl.core.utils import utc_now
from automic_etl.db.engine import get_session
from automic_etl.db.models import (
    NotificationChannelModel,
    AlertRuleModel,
    AlertHistoryModel,
)


class AlertService:
    """Service for managing alerts and notifications in the database."""

    # Notification Channels

    def create_channel(
        self,
        name: str,
        channel_type: str,
        config: Optional[dict] = None,
        created_by: Optional[str] = None,
    ) -> NotificationChannelModel:
        """Create a new notification channel."""
        with get_session() as session:
            channel = NotificationChannelModel(
                id=str(uuid.uuid4()),
                name=name,
                channel_type=channel_type,
                config=config or {},
                created_by=created_by,
            )
            session.add(channel)
            session.flush()
            session.expunge(channel)
            return channel

    def get_channel(self, channel_id: str) -> Optional[NotificationChannelModel]:
        """Get a channel by ID."""
        with get_session() as session:
            channel = session.query(NotificationChannelModel).filter(
                NotificationChannelModel.id == channel_id
            ).first()
            if channel:
                session.expunge(channel)
            return channel

    def list_channels(
        self,
        channel_type: Optional[str] = None,
        enabled: Optional[bool] = None,
    ) -> List[NotificationChannelModel]:
        """List channels with optional filters."""
        with get_session() as session:
            query = session.query(NotificationChannelModel)

            if channel_type:
                query = query.filter(NotificationChannelModel.channel_type == channel_type)
            if enabled is not None:
                query = query.filter(NotificationChannelModel.enabled == enabled)

            channels = query.order_by(NotificationChannelModel.name.asc()).all()
            for c in channels:
                session.expunge(c)
            return channels

    def update_channel(
        self,
        channel_id: str,
        name: Optional[str] = None,
        config: Optional[dict] = None,
        enabled: Optional[bool] = None,
    ) -> Optional[NotificationChannelModel]:
        """Update a channel."""
        with get_session() as session:
            channel = session.query(NotificationChannelModel).filter(
                NotificationChannelModel.id == channel_id
            ).first()

            if not channel:
                return None

            if name is not None:
                channel.name = name
            if config is not None:
                channel.config = config
            if enabled is not None:
                channel.enabled = enabled

            channel.updated_at = utc_now()
            session.flush()
            session.expunge(channel)
            return channel

    def delete_channel(self, channel_id: str) -> bool:
        """Delete a channel."""
        with get_session() as session:
            channel = session.query(NotificationChannelModel).filter(
                NotificationChannelModel.id == channel_id
            ).first()

            if not channel:
                return False

            session.delete(channel)
            return True

    def mark_channel_used(
        self,
        channel_id: str,
        success: bool,
    ) -> None:
        """Mark a channel as used and update status."""
        with get_session() as session:
            channel = session.query(NotificationChannelModel).filter(
                NotificationChannelModel.id == channel_id
            ).first()

            if channel:
                channel.last_used_at = utc_now()
                channel.last_status = "success" if success else "failed"

    # Alert Rules

    def create_alert_rule(
        self,
        name: str,
        rule_type: str,
        condition: Optional[dict] = None,
        severity: str = "warning",
        channels: Optional[list] = None,
        description: str = "",
        cooldown_minutes: int = 15,
        created_by: Optional[str] = None,
    ) -> AlertRuleModel:
        """Create a new alert rule."""
        with get_session() as session:
            rule = AlertRuleModel(
                id=str(uuid.uuid4()),
                name=name,
                description=description,
                rule_type=rule_type,
                condition=condition or {},
                severity=severity,
                channels=channels or [],
                cooldown_minutes=cooldown_minutes,
                created_by=created_by,
            )
            session.add(rule)
            session.flush()
            session.expunge(rule)
            return rule

    def get_alert_rule(self, rule_id: str) -> Optional[AlertRuleModel]:
        """Get an alert rule by ID."""
        with get_session() as session:
            rule = session.query(AlertRuleModel).filter(
                AlertRuleModel.id == rule_id
            ).first()
            if rule:
                session.expunge(rule)
            return rule

    def list_alert_rules(
        self,
        rule_type: Optional[str] = None,
        enabled: Optional[bool] = None,
        severity: Optional[str] = None,
    ) -> List[AlertRuleModel]:
        """List alert rules with optional filters."""
        with get_session() as session:
            query = session.query(AlertRuleModel)

            if rule_type:
                query = query.filter(AlertRuleModel.rule_type == rule_type)
            if enabled is not None:
                query = query.filter(AlertRuleModel.enabled == enabled)
            if severity:
                query = query.filter(AlertRuleModel.severity == severity)

            rules = query.order_by(AlertRuleModel.name.asc()).all()
            for r in rules:
                session.expunge(r)
            return rules

    def get_rules_by_type(self, rule_type: str) -> List[AlertRuleModel]:
        """Get all enabled rules of a specific type."""
        with get_session() as session:
            rules = session.query(AlertRuleModel).filter(
                AlertRuleModel.rule_type == rule_type,
                AlertRuleModel.enabled == True,
            ).all()

            for r in rules:
                session.expunge(r)
            return rules

    def update_alert_rule(
        self,
        rule_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        condition: Optional[dict] = None,
        severity: Optional[str] = None,
        channels: Optional[list] = None,
        enabled: Optional[bool] = None,
        cooldown_minutes: Optional[int] = None,
    ) -> Optional[AlertRuleModel]:
        """Update an alert rule."""
        with get_session() as session:
            rule = session.query(AlertRuleModel).filter(
                AlertRuleModel.id == rule_id
            ).first()

            if not rule:
                return None

            if name is not None:
                rule.name = name
            if description is not None:
                rule.description = description
            if condition is not None:
                rule.condition = condition
            if severity is not None:
                rule.severity = severity
            if channels is not None:
                rule.channels = channels
            if enabled is not None:
                rule.enabled = enabled
            if cooldown_minutes is not None:
                rule.cooldown_minutes = cooldown_minutes

            rule.updated_at = utc_now()
            session.flush()
            session.expunge(rule)
            return rule

    def delete_alert_rule(self, rule_id: str) -> bool:
        """Delete an alert rule."""
        with get_session() as session:
            rule = session.query(AlertRuleModel).filter(
                AlertRuleModel.id == rule_id
            ).first()

            if not rule:
                return False

            session.delete(rule)
            return True

    def can_trigger_alert(self, rule_id: str) -> bool:
        """Check if an alert rule can be triggered (not in cooldown)."""
        with get_session() as session:
            rule = session.query(AlertRuleModel).filter(
                AlertRuleModel.id == rule_id
            ).first()

            if not rule or not rule.enabled:
                return False

            if rule.last_triggered_at is None:
                return True

            cooldown_end = rule.last_triggered_at + timedelta(minutes=rule.cooldown_minutes)
            return utc_now() >= cooldown_end

    def mark_rule_triggered(self, rule_id: str) -> None:
        """Mark an alert rule as triggered."""
        with get_session() as session:
            rule = session.query(AlertRuleModel).filter(
                AlertRuleModel.id == rule_id
            ).first()

            if rule:
                rule.last_triggered_at = utc_now()
                rule.trigger_count = (rule.trigger_count or 0) + 1

    # Alert History

    def create_alert(
        self,
        title: str,
        message: str,
        severity: str,
        rule_id: Optional[str] = None,
        source: Optional[str] = None,
        details: Optional[dict] = None,
    ) -> AlertHistoryModel:
        """Create an alert history record."""
        with get_session() as session:
            alert = AlertHistoryModel(
                id=str(uuid.uuid4()),
                rule_id=rule_id,
                title=title,
                message=message,
                severity=severity,
                source=source,
                details=details or {},
            )
            session.add(alert)
            session.flush()
            session.expunge(alert)
            return alert

    def get_alert(self, alert_id: str) -> Optional[AlertHistoryModel]:
        """Get an alert by ID."""
        with get_session() as session:
            alert = session.query(AlertHistoryModel).filter(
                AlertHistoryModel.id == alert_id
            ).first()
            if alert:
                session.expunge(alert)
            return alert

    def list_alerts(
        self,
        status: Optional[str] = None,
        severity: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100,
    ) -> List[AlertHistoryModel]:
        """List alerts with optional filters."""
        with get_session() as session:
            query = session.query(AlertHistoryModel)

            if status:
                query = query.filter(AlertHistoryModel.status == status)
            if severity:
                query = query.filter(AlertHistoryModel.severity == severity)
            if since:
                query = query.filter(AlertHistoryModel.triggered_at >= since)

            alerts = query.order_by(
                AlertHistoryModel.triggered_at.desc()
            ).limit(limit).all()

            for a in alerts:
                session.expunge(a)
            return alerts

    def get_active_alerts(self) -> List[AlertHistoryModel]:
        """Get all unresolved alerts."""
        with get_session() as session:
            alerts = session.query(AlertHistoryModel).filter(
                AlertHistoryModel.status.in_(["triggered", "acknowledged"])
            ).order_by(
                AlertHistoryModel.triggered_at.desc()
            ).all()

            for a in alerts:
                session.expunge(a)
            return alerts

    def acknowledge_alert(
        self,
        alert_id: str,
        user_id: str,
    ) -> Optional[AlertHistoryModel]:
        """Acknowledge an alert."""
        with get_session() as session:
            alert = session.query(AlertHistoryModel).filter(
                AlertHistoryModel.id == alert_id
            ).first()

            if not alert:
                return None

            alert.status = "acknowledged"
            alert.acknowledged_at = utc_now()
            alert.acknowledged_by = user_id
            session.flush()
            session.expunge(alert)
            return alert

    def resolve_alert(
        self,
        alert_id: str,
        user_id: str,
    ) -> Optional[AlertHistoryModel]:
        """Resolve an alert."""
        with get_session() as session:
            alert = session.query(AlertHistoryModel).filter(
                AlertHistoryModel.id == alert_id
            ).first()

            if not alert:
                return None

            alert.status = "resolved"
            alert.resolved_at = utc_now()
            alert.resolved_by = user_id
            session.flush()
            session.expunge(alert)
            return alert

    def add_notification_sent(
        self,
        alert_id: str,
        channel_id: str,
        channel_type: str,
        success: bool,
    ) -> None:
        """Record that a notification was sent for an alert."""
        with get_session() as session:
            alert = session.query(AlertHistoryModel).filter(
                AlertHistoryModel.id == alert_id
            ).first()

            if alert:
                notifications = alert.notifications_sent or []
                notifications.append({
                    "channel_id": channel_id,
                    "channel_type": channel_type,
                    "success": success,
                    "sent_at": utc_now().isoformat(),
                })
                alert.notifications_sent = notifications

    def get_alert_summary(self) -> dict:
        """Get summary statistics for alerts."""
        with get_session() as session:
            now = utc_now()
            day_ago = now - timedelta(days=1)

            total_active = session.query(AlertHistoryModel).filter(
                AlertHistoryModel.status.in_(["triggered", "acknowledged"])
            ).count()

            critical_active = session.query(AlertHistoryModel).filter(
                AlertHistoryModel.status.in_(["triggered", "acknowledged"]),
                AlertHistoryModel.severity == "critical",
            ).count()

            resolved_24h = session.query(AlertHistoryModel).filter(
                AlertHistoryModel.status == "resolved",
                AlertHistoryModel.resolved_at >= day_ago,
            ).count()

            triggered_24h = session.query(AlertHistoryModel).filter(
                AlertHistoryModel.triggered_at >= day_ago,
            ).count()

            return {
                "total_active": total_active,
                "critical_active": critical_active,
                "resolved_24h": resolved_24h,
                "triggered_24h": triggered_24h,
            }


# Singleton instance
_alert_service: Optional[AlertService] = None


def get_alert_service() -> AlertService:
    """Get the alert service singleton."""
    global _alert_service
    if _alert_service is None:
        _alert_service = AlertService()
    return _alert_service
