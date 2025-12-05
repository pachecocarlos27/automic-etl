"""Session management for authentication."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any
import secrets
import hashlib

import structlog

from automic_etl.core.utils import utc_now

logger = structlog.get_logger()


@dataclass
class Session:
    """User session."""
    session_id: str
    user_id: str
    token_hash: str
    created_at: datetime
    expires_at: datetime
    last_activity: datetime
    ip_address: str | None = None
    user_agent: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def is_expired(self) -> bool:
        """Check if session is expired."""
        return utc_now() > self.expires_at

    @property
    def is_active(self) -> bool:
        """Check if session is active."""
        return not self.is_expired

    def refresh(self, extend_hours: int = 24) -> None:
        """Refresh session expiry."""
        self.last_activity = utc_now()
        self.expires_at = utc_now() + timedelta(hours=extend_hours)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "session_id": self.session_id,
            "user_id": self.user_id,
            "created_at": self.created_at.isoformat(),
            "expires_at": self.expires_at.isoformat(),
            "last_activity": self.last_activity.isoformat(),
            "ip_address": self.ip_address,
            "is_active": self.is_active,
        }


class SessionManager:
    """
    Manage user sessions.

    Features:
    - Session creation and validation
    - Token-based authentication
    - Session expiry management
    - Concurrent session limits
    """

    def __init__(
        self,
        session_duration_hours: int = 24,
        max_sessions_per_user: int = 5,
        inactivity_timeout_hours: int = 2,
    ) -> None:
        """
        Initialize session manager.

        Args:
            session_duration_hours: Session duration in hours
            max_sessions_per_user: Max concurrent sessions per user
            inactivity_timeout_hours: Inactivity timeout
        """
        self.session_duration_hours = session_duration_hours
        self.max_sessions_per_user = max_sessions_per_user
        self.inactivity_timeout_hours = inactivity_timeout_hours
        self.sessions: dict[str, Session] = {}
        self.user_sessions: dict[str, list[str]] = {}
        self.logger = logger.bind(component="session_manager")

    def create_session(
        self,
        user_id: str,
        ip_address: str | None = None,
        user_agent: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> tuple[str, Session]:
        """
        Create a new session.

        Args:
            user_id: User ID
            ip_address: Client IP address
            user_agent: Client user agent
            metadata: Additional metadata

        Returns:
            Tuple of (token, session)
        """
        # Generate secure token
        token = secrets.token_urlsafe(64)
        token_hash = self._hash_token(token)
        session_id = secrets.token_hex(16)

        now = utc_now()

        session = Session(
            session_id=session_id,
            user_id=user_id,
            token_hash=token_hash,
            created_at=now,
            expires_at=now + timedelta(hours=self.session_duration_hours),
            last_activity=now,
            ip_address=ip_address,
            user_agent=user_agent,
            metadata=metadata or {},
        )

        # Enforce max sessions per user
        self._enforce_session_limit(user_id)

        # Store session
        self.sessions[session_id] = session

        if user_id not in self.user_sessions:
            self.user_sessions[user_id] = []
        self.user_sessions[user_id].append(session_id)

        self.logger.info(
            "Session created",
            session_id=session_id,
            user_id=user_id,
        )

        return token, session

    def validate_token(self, token: str) -> Session | None:
        """
        Validate a session token.

        Args:
            token: Session token

        Returns:
            Session if valid, None otherwise
        """
        token_hash = self._hash_token(token)

        for session in self.sessions.values():
            if secrets.compare_digest(session.token_hash, token_hash):
                if session.is_expired:
                    self.invalidate_session(session.session_id)
                    return None

                # Check inactivity
                inactivity = utc_now() - session.last_activity
                if inactivity > timedelta(hours=self.inactivity_timeout_hours):
                    self.invalidate_session(session.session_id)
                    return None

                # Update last activity
                session.last_activity = utc_now()
                return session

        return None

    def get_session(self, session_id: str) -> Session | None:
        """Get session by ID."""
        return self.sessions.get(session_id)

    def invalidate_session(self, session_id: str) -> bool:
        """Invalidate a session."""
        session = self.sessions.get(session_id)
        if not session:
            return False

        del self.sessions[session_id]

        if session.user_id in self.user_sessions:
            if session_id in self.user_sessions[session.user_id]:
                self.user_sessions[session.user_id].remove(session_id)

        self.logger.info("Session invalidated", session_id=session_id)
        return True

    def invalidate_user_sessions(self, user_id: str) -> int:
        """Invalidate all sessions for a user."""
        session_ids = self.user_sessions.get(user_id, []).copy()
        count = 0

        for session_id in session_ids:
            if self.invalidate_session(session_id):
                count += 1

        self.logger.info(
            "User sessions invalidated",
            user_id=user_id,
            count=count,
        )
        return count

    def get_user_sessions(self, user_id: str) -> list[Session]:
        """Get all sessions for a user."""
        session_ids = self.user_sessions.get(user_id, [])
        return [
            self.sessions[sid]
            for sid in session_ids
            if sid in self.sessions
        ]

    def refresh_session(self, session_id: str) -> bool:
        """Refresh a session's expiry."""
        session = self.sessions.get(session_id)
        if session and session.is_active:
            session.refresh(self.session_duration_hours)
            return True
        return False

    def cleanup_expired(self) -> int:
        """Clean up expired sessions."""
        expired = [
            sid for sid, session in self.sessions.items()
            if session.is_expired
        ]

        for session_id in expired:
            self.invalidate_session(session_id)

        if expired:
            self.logger.info("Cleaned up expired sessions", count=len(expired))

        return len(expired)

    def _enforce_session_limit(self, user_id: str) -> None:
        """Enforce maximum sessions per user."""
        user_session_ids = self.user_sessions.get(user_id, [])

        if len(user_session_ids) >= self.max_sessions_per_user:
            # Remove oldest session
            oldest_id = user_session_ids[0]
            self.invalidate_session(oldest_id)
            self.logger.info(
                "Session limit enforced",
                user_id=user_id,
                removed_session=oldest_id,
            )

    @staticmethod
    def _hash_token(token: str) -> str:
        """Hash a token."""
        return hashlib.sha256(token.encode()).hexdigest()

    def get_statistics(self) -> dict[str, Any]:
        """Get session statistics."""
        active_sessions = [s for s in self.sessions.values() if s.is_active]

        return {
            "total_sessions": len(self.sessions),
            "active_sessions": len(active_sessions),
            "unique_users": len(self.user_sessions),
            "expired_sessions": len(self.sessions) - len(active_sessions),
        }
