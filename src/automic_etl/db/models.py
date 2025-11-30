"""SQLAlchemy database models."""

from __future__ import annotations

from datetime import datetime
from typing import Optional
import uuid
import hashlib
import secrets

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum as SQLEnum,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    JSON,
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    """Base class for all models."""
    pass


class UserModel(Base):
    """User account model."""
    __tablename__ = "users"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    username = Column(String(100), unique=True, nullable=False, index=True)
    email = Column(String(255), unique=True, nullable=False, index=True)
    password_hash = Column(String(256), nullable=False)
    salt = Column(String(64), nullable=False)
    first_name = Column(String(100), default="")
    last_name = Column(String(100), default="")
    status = Column(String(20), default="pending")
    roles = Column(JSON, default=list)
    is_superadmin = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_login = Column(DateTime, nullable=True)
    failed_login_attempts = Column(Integer, default=0)
    locked_until = Column(DateTime, nullable=True)
    mfa_enabled = Column(Boolean, default=False)
    mfa_secret = Column(String(100), nullable=True)
    settings = Column(JSON, default=dict)
    metadata_ = Column("metadata", JSON, default=dict)

    sessions = relationship("SessionModel", back_populates="user", cascade="all, delete-orphan")
    pipelines = relationship("PipelineModel", back_populates="owner", cascade="all, delete-orphan")

    @staticmethod
    def hash_password(password: str, salt: str) -> str:
        """Hash a password with salt using PBKDF2."""
        return hashlib.pbkdf2_hmac(
            'sha256',
            password.encode('utf-8'),
            salt.encode('utf-8'),
            100000
        ).hex()

    @classmethod
    def create_user(
        cls,
        username: str,
        email: str,
        password: str,
        first_name: str = "",
        last_name: str = "",
        is_superadmin: bool = False,
    ) -> "UserModel":
        """Create a new user with hashed password."""
        salt = secrets.token_hex(32)
        password_hash = cls.hash_password(password, salt)
        
        return cls(
            id=str(uuid.uuid4()),
            username=username,
            email=email,
            password_hash=password_hash,
            salt=salt,
            first_name=first_name,
            last_name=last_name,
            is_superadmin=is_superadmin,
            status="active" if is_superadmin else "pending",
            roles=["superadmin"] if is_superadmin else ["viewer"],
        )

    def verify_password(self, password: str) -> bool:
        """Verify a password against the stored hash."""
        test_hash = self.hash_password(password, self.salt)
        return secrets.compare_digest(test_hash, self.password_hash)

    def set_password(self, password: str) -> None:
        """Set a new password."""
        self.salt = secrets.token_hex(32)
        self.password_hash = self.hash_password(password, self.salt)
        self.updated_at = datetime.utcnow()

    @property
    def full_name(self) -> str:
        """Get full name."""
        if self.first_name and self.last_name:
            return f"{self.first_name} {self.last_name}"
        return self.first_name or self.last_name or self.username

    @property
    def is_locked(self) -> bool:
        """Check if account is locked."""
        if self.locked_until and datetime.utcnow() < self.locked_until:
            return True
        return False


class SessionModel(Base):
    """User session model for persistent sessions."""
    __tablename__ = "sessions"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String(36), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    token = Column(String(256), unique=True, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime, nullable=False)
    ip_address = Column(String(45), nullable=True)
    user_agent = Column(Text, nullable=True)
    is_valid = Column(Boolean, default=True)

    user = relationship("UserModel", back_populates="sessions")


class PipelineModel(Base):
    """Data pipeline model."""
    __tablename__ = "pipelines"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String(200), nullable=False)
    description = Column(Text, default="")
    owner_id = Column(String(36), ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    status = Column(String(20), default="draft")
    schedule = Column(String(100), nullable=True)
    source_type = Column(String(50), nullable=True)
    source_config = Column(JSON, default=dict)
    destination_layer = Column(String(20), default="bronze")
    transformations = Column(JSON, default=list)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_run_at = Column(DateTime, nullable=True)
    run_count = Column(Integer, default=0)

    owner = relationship("UserModel", back_populates="pipelines")
    runs = relationship("PipelineRunModel", back_populates="pipeline", cascade="all, delete-orphan")


class PipelineRunModel(Base):
    """Pipeline execution run model."""
    __tablename__ = "pipeline_runs"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    pipeline_id = Column(String(36), ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False)
    status = Column(String(20), default="pending")
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    duration_seconds = Column(Float, nullable=True)
    records_processed = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    logs = Column(JSON, default=list)

    pipeline = relationship("PipelineModel", back_populates="runs")


class AuditLogModel(Base):
    """Audit log model for tracking user actions."""
    __tablename__ = "audit_logs"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    user_id = Column(String(36), nullable=True)
    username = Column(String(100), nullable=True)
    action = Column(String(50), nullable=False)
    resource_type = Column(String(50), nullable=True)
    resource_id = Column(String(36), nullable=True)
    details = Column(JSON, default=dict)
    ip_address = Column(String(45), nullable=True)
    user_agent = Column(Text, nullable=True)
    success = Column(Boolean, default=True)


class DataTableModel(Base):
    """Metadata for data tables in the lakehouse."""
    __tablename__ = "data_tables"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String(200), nullable=False)
    layer = Column(String(20), nullable=False)
    schema_definition = Column(JSON, default=dict)
    row_count = Column(Integer, default=0)
    size_bytes = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    source_pipeline_id = Column(String(36), nullable=True)
    quality_score = Column(Float, nullable=True)
