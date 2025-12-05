"""Alert management for data pipeline monitoring."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable
from datetime import timedelta
from enum import Enum
import uuid

import structlog

from automic_etl.core.utils import utc_now
from automic_etl.notifications.notifier import (
    Notifier,
    NotificationLevel,
)

logger = structlog.get_logger()


class AlertSeverity(Enum):
    """Alert severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AlertStatus(Enum):
    """Alert status."""
    ACTIVE = "active"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"
    SUPPRESSED = "suppressed"


class AlertConditionType(Enum):
    """Types of alert conditions."""
    THRESHOLD = "threshold"
    ANOMALY = "anomaly"
    MISSING_DATA = "missing_data"
    SCHEMA_DRIFT = "schema_drift"
    QUALITY_DEGRADATION = "quality_degradation"
    PIPELINE_FAILURE = "pipeline_failure"
    CUSTOM = "custom"


@dataclass
class AlertRule:
    """
    A rule that defines when an alert should be triggered.

    Attributes:
        rule_id: Unique rule identifier
        name: Human-readable rule name
        condition_type: Type of condition
        condition: Callable that evaluates the condition
        severity: Alert severity
        channels: Notification channels to use
        cooldown_minutes: Minimum time between alerts
        metadata: Additional rule metadata
    """
    rule_id: str
    name: str
    condition_type: AlertConditionType
    condition: Callable[[dict[str, Any]], bool]
    severity: AlertSeverity = AlertSeverity.MEDIUM
    channels: list[str] | None = None
    cooldown_minutes: int = 15
    enabled: bool = True
    description: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def threshold(
        cls,
        name: str,
        metric: str,
        operator: str,
        value: float,
        severity: AlertSeverity = AlertSeverity.MEDIUM,
        **kwargs: Any,
    ) -> "AlertRule":
        """Create a threshold-based alert rule."""
        operators = {
            ">": lambda x, v: x > v,
            ">=": lambda x, v: x >= v,
            "<": lambda x, v: x < v,
            "<=": lambda x, v: x <= v,
            "==": lambda x, v: x == v,
            "!=": lambda x, v: x != v,
        }

        if operator not in operators:
            raise ValueError(f"Invalid operator: {operator}")

        op_func = operators[operator]

        def condition(context: dict[str, Any]) -> bool:
            metric_value = context.get(metric)
            if metric_value is None:
                return False
            return op_func(metric_value, value)

        return cls(
            rule_id=str(uuid.uuid4()),
            name=name,
            condition_type=AlertConditionType.THRESHOLD,
            condition=condition,
            severity=severity,
            description=f"{metric} {operator} {value}",
            metadata={"metric": metric, "operator": operator, "value": value},
            **kwargs,
        )

    @classmethod
    def quality_degradation(
        cls,
        name: str,
        min_quality_score: float = 80.0,
        severity: AlertSeverity = AlertSeverity.HIGH,
        **kwargs: Any,
    ) -> "AlertRule":
        """Create an alert rule for quality score drops."""
        def condition(context: dict[str, Any]) -> bool:
            score = context.get("quality_score")
            if score is None:
                return False
            return score < min_quality_score

        return cls(
            rule_id=str(uuid.uuid4()),
            name=name,
            condition_type=AlertConditionType.QUALITY_DEGRADATION,
            condition=condition,
            severity=severity,
            description=f"Quality score below {min_quality_score}",
            metadata={"min_quality_score": min_quality_score},
            **kwargs,
        )

    @classmethod
    def missing_data(
        cls,
        name: str,
        max_null_percentage: float = 50.0,
        columns: list[str] | None = None,
        severity: AlertSeverity = AlertSeverity.MEDIUM,
        **kwargs: Any,
    ) -> "AlertRule":
        """Create an alert rule for missing data."""
        def condition(context: dict[str, Any]) -> bool:
            null_pct = context.get("null_percentage")
            col = context.get("column")

            if null_pct is None:
                return False
            if columns and col and col not in columns:
                return False
            return null_pct > max_null_percentage

        return cls(
            rule_id=str(uuid.uuid4()),
            name=name,
            condition_type=AlertConditionType.MISSING_DATA,
            condition=condition,
            severity=severity,
            description=f"Null percentage exceeds {max_null_percentage}%",
            metadata={"max_null_percentage": max_null_percentage, "columns": columns},
            **kwargs,
        )

    @classmethod
    def schema_drift(
        cls,
        name: str,
        severity: AlertSeverity = AlertSeverity.HIGH,
        **kwargs: Any,
    ) -> "AlertRule":
        """Create an alert rule for schema changes."""
        def condition(context: dict[str, Any]) -> bool:
            return context.get("schema_changed", False)

        return cls(
            rule_id=str(uuid.uuid4()),
            name=name,
            condition_type=AlertConditionType.SCHEMA_DRIFT,
            condition=condition,
            severity=severity,
            description="Schema change detected",
            **kwargs,
        )

    @classmethod
    def pipeline_failure(
        cls,
        name: str,
        pipeline_pattern: str | None = None,
        severity: AlertSeverity = AlertSeverity.CRITICAL,
        **kwargs: Any,
    ) -> "AlertRule":
        """Create an alert rule for pipeline failures."""
        import re

        def condition(context: dict[str, Any]) -> bool:
            if context.get("status") != "failed":
                return False
            if pipeline_pattern:
                pipeline_name = context.get("pipeline_name", "")
                return bool(re.match(pipeline_pattern, pipeline_name))
            return True

        return cls(
            rule_id=str(uuid.uuid4()),
            name=name,
            condition_type=AlertConditionType.PIPELINE_FAILURE,
            condition=condition,
            severity=severity,
            description=f"Pipeline failure{f' (pattern: {pipeline_pattern})' if pipeline_pattern else ''}",
            metadata={"pipeline_pattern": pipeline_pattern},
            **kwargs,
        )


@dataclass
class Alert:
    """
    An alert instance triggered by a rule.

    Attributes:
        alert_id: Unique alert identifier
        rule: The rule that triggered this alert
        status: Current alert status
        triggered_at: When the alert was triggered
        context: Context data when triggered
        message: Alert message
        acknowledged_at: When acknowledged
        acknowledged_by: Who acknowledged
        resolved_at: When resolved
    """
    alert_id: str
    rule: AlertRule
    status: AlertStatus
    triggered_at: datetime
    context: dict[str, Any]
    message: str
    acknowledged_at: datetime | None = None
    acknowledged_by: str | None = None
    resolved_at: datetime | None = None
    notification_sent: bool = False

    def acknowledge(self, by: str) -> None:
        """Acknowledge the alert."""
        self.status = AlertStatus.ACKNOWLEDGED
        self.acknowledged_at = utc_now()
        self.acknowledged_by = by

    def resolve(self) -> None:
        """Mark the alert as resolved."""
        self.status = AlertStatus.RESOLVED
        self.resolved_at = utc_now()

    def suppress(self) -> None:
        """Suppress the alert."""
        self.status = AlertStatus.SUPPRESSED

    @property
    def duration(self) -> timedelta | None:
        """Get alert duration."""
        if self.resolved_at:
            return self.resolved_at - self.triggered_at
        return utc_now() - self.triggered_at

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "alert_id": self.alert_id,
            "rule_id": self.rule.rule_id,
            "rule_name": self.rule.name,
            "severity": self.rule.severity.value,
            "condition_type": self.rule.condition_type.value,
            "status": self.status.value,
            "triggered_at": self.triggered_at.isoformat(),
            "message": self.message,
            "context": self.context,
            "acknowledged_at": self.acknowledged_at.isoformat() if self.acknowledged_at else None,
            "acknowledged_by": self.acknowledged_by,
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
        }


class AlertManager:
    """
    Manages alert rules and triggered alerts.

    Features:
    - Rule-based alerting
    - Alert deduplication
    - Cooldown periods
    - Integration with notification system
    - Alert history tracking
    """

    SEVERITY_TO_LEVEL = {
        AlertSeverity.LOW: NotificationLevel.INFO,
        AlertSeverity.MEDIUM: NotificationLevel.WARNING,
        AlertSeverity.HIGH: NotificationLevel.ERROR,
        AlertSeverity.CRITICAL: NotificationLevel.CRITICAL,
    }

    def __init__(self, notifier: Notifier | None = None) -> None:
        """
        Initialize alert manager.

        Args:
            notifier: Optional notifier for sending alerts
        """
        self.notifier = notifier
        self.rules: dict[str, AlertRule] = {}
        self.alerts: dict[str, Alert] = {}
        self.last_triggered: dict[str, datetime] = {}
        self.logger = logger.bind(component="alert_manager")

    def add_rule(self, rule: AlertRule) -> str:
        """
        Add an alert rule.

        Args:
            rule: Alert rule to add

        Returns:
            Rule ID
        """
        self.rules[rule.rule_id] = rule
        self.logger.info("Alert rule added", rule_id=rule.rule_id, name=rule.name)
        return rule.rule_id

    def remove_rule(self, rule_id: str) -> bool:
        """Remove an alert rule."""
        if rule_id in self.rules:
            del self.rules[rule_id]
            self.logger.info("Alert rule removed", rule_id=rule_id)
            return True
        return False

    def enable_rule(self, rule_id: str) -> bool:
        """Enable an alert rule."""
        if rule_id in self.rules:
            self.rules[rule_id].enabled = True
            return True
        return False

    def disable_rule(self, rule_id: str) -> bool:
        """Disable an alert rule."""
        if rule_id in self.rules:
            self.rules[rule_id].enabled = False
            return True
        return False

    def evaluate(
        self,
        context: dict[str, Any],
        source: str | None = None,
    ) -> list[Alert]:
        """
        Evaluate all rules against the given context.

        Args:
            context: Context data to evaluate
            source: Source of the context data

        Returns:
            List of triggered alerts
        """
        triggered = []
        now = utc_now()

        for rule in self.rules.values():
            if not rule.enabled:
                continue

            # Check cooldown
            last = self.last_triggered.get(rule.rule_id)
            if last:
                cooldown = timedelta(minutes=rule.cooldown_minutes)
                if now - last < cooldown:
                    continue

            # Evaluate condition
            try:
                if rule.condition(context):
                    alert = self._create_alert(rule, context, source)
                    triggered.append(alert)
                    self.last_triggered[rule.rule_id] = now

                    # Send notification
                    if self.notifier:
                        self._send_notification(alert)

            except Exception as e:
                self.logger.error(
                    "Error evaluating rule",
                    rule_id=rule.rule_id,
                    error=str(e),
                )

        return triggered

    def _create_alert(
        self,
        rule: AlertRule,
        context: dict[str, Any],
        source: str | None,
    ) -> Alert:
        """Create an alert from a triggered rule."""
        alert_id = str(uuid.uuid4())

        message = self._format_message(rule, context, source)

        alert = Alert(
            alert_id=alert_id,
            rule=rule,
            status=AlertStatus.ACTIVE,
            triggered_at=utc_now(),
            context=context,
            message=message,
        )

        self.alerts[alert_id] = alert

        self.logger.warning(
            "Alert triggered",
            alert_id=alert_id,
            rule=rule.name,
            severity=rule.severity.value,
        )

        return alert

    def _format_message(
        self,
        rule: AlertRule,
        context: dict[str, Any],
        source: str | None,
    ) -> str:
        """Format alert message."""
        parts = [f"Alert: {rule.name}"]

        if rule.description:
            parts.append(f"Condition: {rule.description}")

        if source:
            parts.append(f"Source: {source}")

        # Add relevant context values
        relevant_keys = ["metric", "value", "expected", "actual", "pipeline_name", "error"]
        for key in relevant_keys:
            if key in context:
                parts.append(f"{key.replace('_', ' ').title()}: {context[key]}")

        return "\n".join(parts)

    def _send_notification(self, alert: Alert) -> bool:
        """Send notification for an alert."""
        if not self.notifier:
            return False

        level = self.SEVERITY_TO_LEVEL.get(
            alert.rule.severity,
            NotificationLevel.WARNING,
        )

        success = self.notifier.send(
            title=f"[{alert.rule.severity.value.upper()}] {alert.rule.name}",
            message=alert.message,
            level=level,
            channels=alert.rule.channels,
            alert_id=alert.alert_id,
            rule_id=alert.rule.rule_id,
            condition_type=alert.rule.condition_type.value,
        )

        alert.notification_sent = success
        return success

    def acknowledge_alert(self, alert_id: str, by: str) -> bool:
        """Acknowledge an alert."""
        alert = self.alerts.get(alert_id)
        if alert and alert.status == AlertStatus.ACTIVE:
            alert.acknowledge(by)
            self.logger.info("Alert acknowledged", alert_id=alert_id, by=by)
            return True
        return False

    def resolve_alert(self, alert_id: str) -> bool:
        """Resolve an alert."""
        alert = self.alerts.get(alert_id)
        if alert and alert.status in (AlertStatus.ACTIVE, AlertStatus.ACKNOWLEDGED):
            alert.resolve()
            self.logger.info("Alert resolved", alert_id=alert_id)
            return True
        return False

    def get_active_alerts(self) -> list[Alert]:
        """Get all active alerts."""
        return [
            a for a in self.alerts.values()
            if a.status in (AlertStatus.ACTIVE, AlertStatus.ACKNOWLEDGED)
        ]

    def get_alerts_by_severity(self, severity: AlertSeverity) -> list[Alert]:
        """Get alerts by severity."""
        return [
            a for a in self.alerts.values()
            if a.rule.severity == severity
        ]

    def get_alert_history(
        self,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        rule_id: str | None = None,
    ) -> list[Alert]:
        """Get alert history with optional filters."""
        alerts = list(self.alerts.values())

        if start_time:
            alerts = [a for a in alerts if a.triggered_at >= start_time]
        if end_time:
            alerts = [a for a in alerts if a.triggered_at <= end_time]
        if rule_id:
            alerts = [a for a in alerts if a.rule.rule_id == rule_id]

        return sorted(alerts, key=lambda a: a.triggered_at, reverse=True)

    def get_statistics(self) -> dict[str, Any]:
        """Get alert statistics."""
        alerts = list(self.alerts.values())

        by_severity = {}
        for severity in AlertSeverity:
            by_severity[severity.value] = len([
                a for a in alerts if a.rule.severity == severity
            ])

        by_status = {}
        for status in AlertStatus:
            by_status[status.value] = len([
                a for a in alerts if a.status == status
            ])

        by_type = {}
        for ctype in AlertConditionType:
            by_type[ctype.value] = len([
                a for a in alerts if a.rule.condition_type == ctype
            ])

        return {
            "total_alerts": len(alerts),
            "active_alerts": len(self.get_active_alerts()),
            "total_rules": len(self.rules),
            "enabled_rules": len([r for r in self.rules.values() if r.enabled]),
            "by_severity": by_severity,
            "by_status": by_status,
            "by_type": by_type,
        }

    def clear_resolved(self, older_than_hours: int = 24) -> int:
        """Clear resolved alerts older than specified hours."""
        cutoff = utc_now() - timedelta(hours=older_than_hours)
        to_remove = [
            a.alert_id for a in self.alerts.values()
            if a.status == AlertStatus.RESOLVED and a.resolved_at and a.resolved_at < cutoff
        ]

        for alert_id in to_remove:
            del self.alerts[alert_id]

        return len(to_remove)
