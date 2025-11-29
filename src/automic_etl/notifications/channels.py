"""Notification channel implementations."""

from __future__ import annotations

from typing import Any
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import httpx
import structlog

from automic_etl.notifications.notifier import (
    NotificationChannel,
    Notification,
    NotificationLevel,
)

logger = structlog.get_logger()


class EmailNotifier(NotificationChannel):
    """Email notification channel."""

    def __init__(
        self,
        smtp_host: str,
        smtp_port: int,
        username: str | None = None,
        password: str | None = None,
        from_address: str = "automic-etl@localhost",
        to_addresses: list[str] | None = None,
        use_tls: bool = True,
    ) -> None:
        """
        Initialize email notifier.

        Args:
            smtp_host: SMTP server host
            smtp_port: SMTP server port
            username: SMTP username
            password: SMTP password
            from_address: Sender email address
            to_addresses: Recipient email addresses
            use_tls: Use TLS encryption
        """
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.from_address = from_address
        self.to_addresses = to_addresses or []
        self.use_tls = use_tls
        self.logger = logger.bind(channel="email")

    def send(self, notification: Notification) -> bool:
        """Send email notification."""
        if not self.to_addresses:
            self.logger.warning("No recipients configured")
            return False

        # Build email
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"[{notification.level.value.upper()}] {notification.title}"
        msg["From"] = self.from_address
        msg["To"] = ", ".join(self.to_addresses)

        # Plain text
        text_content = f"""
{notification.title}
{'=' * len(notification.title)}

Level: {notification.level.value}
Time: {notification.timestamp.isoformat()}
Source: {notification.source or 'N/A'}

{notification.message}

Metadata:
{self._format_metadata(notification.metadata)}
"""

        # HTML version
        html_content = f"""
<html>
<body>
<h2>{notification.title}</h2>
<p><strong>Level:</strong> {notification.level.value}</p>
<p><strong>Time:</strong> {notification.timestamp.isoformat()}</p>
<p><strong>Source:</strong> {notification.source or 'N/A'}</p>
<hr>
<p>{notification.message.replace(chr(10), '<br>')}</p>
<hr>
<h4>Metadata</h4>
<pre>{self._format_metadata(notification.metadata)}</pre>
</body>
</html>
"""

        msg.attach(MIMEText(text_content, "plain"))
        msg.attach(MIMEText(html_content, "html"))

        try:
            if self.use_tls:
                server = smtplib.SMTP(self.smtp_host, self.smtp_port)
                server.starttls()
            else:
                server = smtplib.SMTP(self.smtp_host, self.smtp_port)

            if self.username and self.password:
                server.login(self.username, self.password)

            server.sendmail(self.from_address, self.to_addresses, msg.as_string())
            server.quit()

            self.logger.debug("Email sent", recipients=len(self.to_addresses))
            return True

        except Exception as e:
            self.logger.error("Failed to send email", error=str(e))
            return False

    def _format_metadata(self, metadata: dict) -> str:
        return "\n".join(f"  {k}: {v}" for k, v in metadata.items())

    def test_connection(self) -> bool:
        """Test SMTP connection."""
        try:
            if self.use_tls:
                server = smtplib.SMTP(self.smtp_host, self.smtp_port)
                server.starttls()
            else:
                server = smtplib.SMTP(self.smtp_host, self.smtp_port)

            if self.username and self.password:
                server.login(self.username, self.password)

            server.quit()
            return True
        except Exception:
            return False


class SlackNotifier(NotificationChannel):
    """Slack notification channel using webhooks."""

    LEVEL_COLORS = {
        NotificationLevel.DEBUG: "#808080",
        NotificationLevel.INFO: "#2196F3",
        NotificationLevel.WARNING: "#FF9800",
        NotificationLevel.ERROR: "#F44336",
        NotificationLevel.CRITICAL: "#9C27B0",
    }

    LEVEL_EMOJIS = {
        NotificationLevel.DEBUG: ":mag:",
        NotificationLevel.INFO: ":information_source:",
        NotificationLevel.WARNING: ":warning:",
        NotificationLevel.ERROR: ":x:",
        NotificationLevel.CRITICAL: ":rotating_light:",
    }

    def __init__(
        self,
        webhook_url: str,
        channel: str | None = None,
        username: str = "Automic ETL",
        icon_emoji: str = ":robot_face:",
    ) -> None:
        """
        Initialize Slack notifier.

        Args:
            webhook_url: Slack incoming webhook URL
            channel: Override channel
            username: Bot username
            icon_emoji: Bot icon emoji
        """
        self.webhook_url = webhook_url
        self.channel = channel
        self.username = username
        self.icon_emoji = icon_emoji
        self.logger = logger.bind(channel="slack")

    def send(self, notification: Notification) -> bool:
        """Send Slack notification."""
        emoji = self.LEVEL_EMOJIS.get(notification.level, ":bell:")
        color = self.LEVEL_COLORS.get(notification.level, "#808080")

        payload = {
            "username": self.username,
            "icon_emoji": self.icon_emoji,
            "attachments": [
                {
                    "color": color,
                    "title": f"{emoji} {notification.title}",
                    "text": notification.message,
                    "fields": [
                        {
                            "title": "Level",
                            "value": notification.level.value,
                            "short": True,
                        },
                        {
                            "title": "Time",
                            "value": notification.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                            "short": True,
                        },
                    ],
                    "footer": f"Source: {notification.source or 'Automic ETL'}",
                    "ts": int(notification.timestamp.timestamp()),
                }
            ],
        }

        # Add metadata fields
        if notification.metadata:
            for key, value in list(notification.metadata.items())[:5]:
                payload["attachments"][0]["fields"].append({
                    "title": key,
                    "value": str(value)[:100],
                    "short": True,
                })

        if self.channel:
            payload["channel"] = self.channel

        try:
            response = httpx.post(self.webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            self.logger.debug("Slack message sent")
            return True
        except Exception as e:
            self.logger.error("Failed to send Slack message", error=str(e))
            return False

    def test_connection(self) -> bool:
        """Test Slack webhook."""
        try:
            response = httpx.post(
                self.webhook_url,
                json={"text": "Test message from Automic ETL"},
                timeout=10,
            )
            return response.status_code == 200
        except Exception:
            return False


class WebhookNotifier(NotificationChannel):
    """Generic webhook notification channel."""

    def __init__(
        self,
        url: str,
        method: str = "POST",
        headers: dict[str, str] | None = None,
        auth: tuple[str, str] | None = None,
        timeout: int = 30,
    ) -> None:
        """
        Initialize webhook notifier.

        Args:
            url: Webhook URL
            method: HTTP method
            headers: Additional headers
            auth: Basic auth (username, password)
            timeout: Request timeout
        """
        self.url = url
        self.method = method
        self.headers = headers or {}
        self.auth = auth
        self.timeout = timeout
        self.logger = logger.bind(channel="webhook")

    def send(self, notification: Notification) -> bool:
        """Send webhook notification."""
        payload = notification.to_dict()

        try:
            response = httpx.request(
                method=self.method,
                url=self.url,
                json=payload,
                headers=self.headers,
                auth=self.auth,
                timeout=self.timeout,
            )
            response.raise_for_status()
            self.logger.debug("Webhook sent", status=response.status_code)
            return True
        except Exception as e:
            self.logger.error("Failed to send webhook", error=str(e))
            return False

    def test_connection(self) -> bool:
        """Test webhook endpoint."""
        try:
            response = httpx.request(
                method="GET" if self.method == "GET" else "POST",
                url=self.url,
                headers=self.headers,
                auth=self.auth,
                timeout=10,
            )
            return response.status_code < 500
        except Exception:
            return False


class TeamsNotifier(NotificationChannel):
    """Microsoft Teams notification channel."""

    LEVEL_COLORS = {
        NotificationLevel.DEBUG: "808080",
        NotificationLevel.INFO: "0078D7",
        NotificationLevel.WARNING: "FFC107",
        NotificationLevel.ERROR: "DC3545",
        NotificationLevel.CRITICAL: "6F42C1",
    }

    def __init__(
        self,
        webhook_url: str,
    ) -> None:
        """
        Initialize Teams notifier.

        Args:
            webhook_url: Teams incoming webhook URL
        """
        self.webhook_url = webhook_url
        self.logger = logger.bind(channel="teams")

    def send(self, notification: Notification) -> bool:
        """Send Teams notification."""
        color = self.LEVEL_COLORS.get(notification.level, "808080")

        # Build adaptive card
        payload = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": color,
            "summary": notification.title,
            "sections": [
                {
                    "activityTitle": notification.title,
                    "activitySubtitle": f"Level: {notification.level.value}",
                    "activityImage": "https://via.placeholder.com/50",
                    "facts": [
                        {"name": "Time", "value": notification.timestamp.isoformat()},
                        {"name": "Source", "value": notification.source or "Automic ETL"},
                    ],
                    "text": notification.message,
                }
            ],
        }

        # Add metadata facts
        if notification.metadata:
            for key, value in list(notification.metadata.items())[:5]:
                payload["sections"][0]["facts"].append({
                    "name": key,
                    "value": str(value)[:100],
                })

        try:
            response = httpx.post(self.webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            self.logger.debug("Teams message sent")
            return True
        except Exception as e:
            self.logger.error("Failed to send Teams message", error=str(e))
            return False

    def test_connection(self) -> bool:
        """Test Teams webhook."""
        try:
            response = httpx.post(
                self.webhook_url,
                json={
                    "@type": "MessageCard",
                    "summary": "Test",
                    "text": "Test message from Automic ETL",
                },
                timeout=10,
            )
            return response.status_code == 200
        except Exception:
            return False


class ConsoleNotifier(NotificationChannel):
    """Console/stdout notification channel for development."""

    LEVEL_SYMBOLS = {
        NotificationLevel.DEBUG: "🔍",
        NotificationLevel.INFO: "ℹ️",
        NotificationLevel.WARNING: "⚠️",
        NotificationLevel.ERROR: "❌",
        NotificationLevel.CRITICAL: "🚨",
    }

    def send(self, notification: Notification) -> bool:
        """Print notification to console."""
        symbol = self.LEVEL_SYMBOLS.get(notification.level, "📢")

        print(f"\n{symbol} [{notification.level.value.upper()}] {notification.title}")
        print("-" * 50)
        print(notification.message)
        print(f"Time: {notification.timestamp.isoformat()}")
        if notification.metadata:
            print("Metadata:", notification.metadata)
        print()

        return True

    def test_connection(self) -> bool:
        """Console is always available."""
        return True
