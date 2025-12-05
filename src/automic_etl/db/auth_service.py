"""Database-backed authentication service."""

from __future__ import annotations

from datetime import timedelta
import os
import secrets
from typing import Optional, Tuple

import structlog

from automic_etl.core.utils import utc_now
from automic_etl.db.engine import get_session, init_db
from automic_etl.db.models import UserModel, SessionModel, AuditLogModel

logger = structlog.get_logger()

SUPERADMIN_EMAIL_KEY = "SUPERADMIN_EMAIL"
SUPERADMIN_PASSWORD_KEY = "SUPERADMIN_PASSWORD"
SESSION_DURATION_HOURS = 24
MAX_FAILED_ATTEMPTS = 5
LOCKOUT_MINUTES = 30
MIN_PASSWORD_LENGTH = 8


class AuthenticationError(Exception):
    """Authentication error."""
    pass


class AuthService:
    """Database-backed authentication service."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self.logger = logger.bind(component="auth_service")

    def initialize(self) -> None:
        """Initialize database and ensure superadmin exists."""
        init_db()
        self._ensure_superadmin()

    def _ensure_superadmin(self) -> None:
        """Ensure superadmin account exists using environment secrets."""
        superadmin_email = os.environ.get(SUPERADMIN_EMAIL_KEY)
        superadmin_password = os.environ.get(SUPERADMIN_PASSWORD_KEY)

        if not superadmin_email or not superadmin_password:
            self.logger.warning(
                "Superadmin secrets not configured",
                hint=f"Set {SUPERADMIN_EMAIL_KEY} and {SUPERADMIN_PASSWORD_KEY} secrets"
            )
            return

        with get_session() as session:
            existing_user = session.query(UserModel).filter(
                UserModel.email == superadmin_email
            ).first()

            old_superadmins = session.query(UserModel).filter(
                UserModel.is_superadmin == True,
                UserModel.email != superadmin_email
            ).all()
            for old_admin in old_superadmins:
                old_admin.is_superadmin = False
                old_admin.roles = [r for r in (old_admin.roles or []) if r != "superadmin"]
                self.logger.info("Revoked superadmin from old account", email=old_admin.email)

            if existing_user:
                existing_user.is_superadmin = True
                existing_user.status = "active"
                if "superadmin" not in (existing_user.roles or []):
                    existing_user.roles = (existing_user.roles or []) + ["superadmin"]

                if not existing_user.verify_password(superadmin_password):
                    existing_user.set_password(superadmin_password)
                    self.logger.info("Superadmin password updated from secrets")

                self.logger.info("Superadmin configured from secrets", email=superadmin_email)
            else:
                new_user = UserModel.create_user(
                    username=superadmin_email.split("@")[0],
                    email=superadmin_email,
                    password=superadmin_password,
                    first_name="Super",
                    last_name="Admin",
                    is_superadmin=True,
                )
                session.add(new_user)
                self.logger.info("Superadmin created from secrets", email=superadmin_email)

    def register(
        self,
        username: str,
        email: str,
        password: str,
        first_name: str = "",
        last_name: str = "",
        auto_activate: bool = False,
    ) -> UserModel:
        """Register a new user."""
        self._validate_password(password)

        with get_session() as session:
            if session.query(UserModel).filter(UserModel.username == username).first():
                raise AuthenticationError("Username already exists")

            if session.query(UserModel).filter(UserModel.email == email).first():
                raise AuthenticationError("Email already registered")

            user = UserModel.create_user(
                username=username,
                email=email,
                password=password,
                first_name=first_name,
                last_name=last_name,
            )
            
            if auto_activate:
                user.status = "active"

            session.add(user)
            session.flush()

            self._log_action(
                session,
                action="user_created",
                user_id=user.id,
                username=user.username,
                resource_type="user",
                resource_id=user.id,
            )

            self.logger.info("User registered", username=username)
            return user

    def authenticate(
        self,
        username: str,
        password: str,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> Tuple[UserModel, str]:
        """Authenticate a user and return user + session token."""
        with get_session() as session:
            user = session.query(UserModel).filter(
                (UserModel.username == username) | (UserModel.email == username)
            ).first()

            if not user:
                self._log_action(
                    session,
                    action="login_failed",
                    username=username,
                    ip_address=ip_address,
                    details={"reason": "user_not_found"},
                    success=False,
                )
                raise AuthenticationError("Invalid credentials")

            if user.status == "suspended":
                raise AuthenticationError("Account suspended")

            if user.status == "inactive":
                raise AuthenticationError("Account inactive")

            if user.status == "pending":
                raise AuthenticationError("Account pending activation")

            if user.is_locked:
                raise AuthenticationError("Account locked. Try again later.")

            if not user.verify_password(password):
                user.failed_login_attempts += 1
                if user.failed_login_attempts >= MAX_FAILED_ATTEMPTS:
                    user.locked_until = utc_now() + timedelta(minutes=LOCKOUT_MINUTES)

                self._log_action(
                    session,
                    action="login_failed",
                    user_id=user.id,
                    username=user.username,
                    ip_address=ip_address,
                    details={"attempts": user.failed_login_attempts},
                    success=False,
                )
                raise AuthenticationError("Invalid credentials")

            user.last_login = utc_now()
            user.failed_login_attempts = 0
            user.locked_until = None

            token = secrets.token_urlsafe(32)
            user_session = SessionModel(
                user_id=user.id,
                token=token,
                expires_at=utc_now() + timedelta(hours=SESSION_DURATION_HOURS),
                ip_address=ip_address,
                user_agent=user_agent,
            )
            session.add(user_session)

            self._log_action(
                session,
                action="login",
                user_id=user.id,
                username=user.username,
                ip_address=ip_address,
            )

            self.logger.info("User authenticated", username=user.username)
            
            session.expunge(user)
            return user, token

    def validate_session(self, token: str) -> Optional[UserModel]:
        """Validate a session token and return the user if valid."""
        if not token:
            return None

        with get_session() as session:
            user_session = session.query(SessionModel).filter(
                SessionModel.token == token,
                SessionModel.is_valid == True,
                SessionModel.expires_at > utc_now()
            ).first()

            if not user_session:
                return None

            user = session.query(UserModel).filter(
                UserModel.id == user_session.user_id,
                UserModel.status == "active"
            ).first()

            if not user:
                user_session.is_valid = False
                return None

            session.expunge(user)
            return user

    def logout(self, token: str) -> bool:
        """Invalidate a session token."""
        if not token:
            return False

        with get_session() as session:
            user_session = session.query(SessionModel).filter(
                SessionModel.token == token
            ).first()

            if not user_session:
                return False

            user_session.is_valid = False

            self._log_action(
                session,
                action="logout",
                user_id=user_session.user_id,
            )

            return True

    def get_user_by_id(self, user_id: str) -> Optional[UserModel]:
        """Get user by ID."""
        with get_session() as session:
            user = session.query(UserModel).filter(UserModel.id == user_id).first()
            if user:
                session.expunge(user)
            return user

    def change_password(
        self,
        user_id: str,
        current_password: str,
        new_password: str,
    ) -> bool:
        """Change a user's password."""
        self._validate_password(new_password)

        with get_session() as session:
            user = session.query(UserModel).filter(UserModel.id == user_id).first()
            if not user:
                return False

            if not user.verify_password(current_password):
                raise AuthenticationError("Current password incorrect")

            user.set_password(new_password)

            session.query(SessionModel).filter(
                SessionModel.user_id == user_id
            ).update({"is_valid": False})

            self._log_action(
                session,
                action="password_change",
                user_id=user.id,
                username=user.username,
            )

            return True

    def _validate_password(self, password: str) -> None:
        """Validate password meets requirements."""
        if len(password) < MIN_PASSWORD_LENGTH:
            raise AuthenticationError(
                f"Password must be at least {MIN_PASSWORD_LENGTH} characters"
            )

        has_upper = any(c.isupper() for c in password)
        has_lower = any(c.islower() for c in password)
        has_digit = any(c.isdigit() for c in password)

        if not (has_upper and has_lower and has_digit):
            raise AuthenticationError(
                "Password must contain uppercase, lowercase, and numbers"
            )

    def _log_action(
        self,
        session,
        action: str,
        user_id: Optional[str] = None,
        username: Optional[str] = None,
        resource_type: Optional[str] = None,
        resource_id: Optional[str] = None,
        details: Optional[dict] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        success: bool = True,
    ) -> None:
        """Log an action to the audit log."""
        log_entry = AuditLogModel(
            user_id=user_id,
            username=username,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            details=details or {},
            ip_address=ip_address,
            user_agent=user_agent,
            success=success,
        )
        session.add(log_entry)


def get_auth_service() -> AuthService:
    """Get the singleton auth service instance."""
    return AuthService()
