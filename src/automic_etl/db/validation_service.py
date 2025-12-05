"""Database-backed validation service."""

from __future__ import annotations

from typing import Optional, List
import uuid

from automic_etl.core.utils import utc_now
from automic_etl.db.engine import get_session
from automic_etl.db.models import ValidationRuleModel, ValidationResultModel


class ValidationService:
    """Service for managing validation rules and results in the database."""

    # Validation Rules

    def create_rule(
        self,
        name: str,
        rule_type: str,
        target_table: str,
        target_column: Optional[str] = None,
        description: str = "",
        rule_config: Optional[dict] = None,
        severity: str = "warning",
        created_by: Optional[str] = None,
    ) -> ValidationRuleModel:
        """Create a new validation rule."""
        with get_session() as session:
            rule = ValidationRuleModel(
                id=str(uuid.uuid4()),
                name=name,
                description=description,
                rule_type=rule_type,
                target_table=target_table,
                target_column=target_column,
                rule_config=rule_config or {},
                severity=severity,
                created_by=created_by,
            )
            session.add(rule)
            session.flush()
            session.expunge(rule)
            return rule

    def get_rule(self, rule_id: str) -> Optional[ValidationRuleModel]:
        """Get a rule by ID."""
        with get_session() as session:
            rule = session.query(ValidationRuleModel).filter(
                ValidationRuleModel.id == rule_id
            ).first()
            if rule:
                session.expunge(rule)
            return rule

    def list_rules(
        self,
        target_table: Optional[str] = None,
        rule_type: Optional[str] = None,
        enabled: Optional[bool] = None,
        severity: Optional[str] = None,
    ) -> List[ValidationRuleModel]:
        """List rules with optional filters."""
        with get_session() as session:
            query = session.query(ValidationRuleModel)

            if target_table:
                query = query.filter(ValidationRuleModel.target_table == target_table)
            if rule_type:
                query = query.filter(ValidationRuleModel.rule_type == rule_type)
            if enabled is not None:
                query = query.filter(ValidationRuleModel.enabled == enabled)
            if severity:
                query = query.filter(ValidationRuleModel.severity == severity)

            rules = query.order_by(ValidationRuleModel.name.asc()).all()
            for r in rules:
                session.expunge(r)
            return rules

    def get_rules_for_table(self, table_name: str) -> List[ValidationRuleModel]:
        """Get all enabled rules for a specific table."""
        with get_session() as session:
            rules = session.query(ValidationRuleModel).filter(
                ValidationRuleModel.target_table == table_name,
                ValidationRuleModel.enabled == True,
            ).all()

            for r in rules:
                session.expunge(r)
            return rules

    def update_rule(
        self,
        rule_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        rule_config: Optional[dict] = None,
        severity: Optional[str] = None,
        enabled: Optional[bool] = None,
    ) -> Optional[ValidationRuleModel]:
        """Update a rule."""
        with get_session() as session:
            rule = session.query(ValidationRuleModel).filter(
                ValidationRuleModel.id == rule_id
            ).first()

            if not rule:
                return None

            if name is not None:
                rule.name = name
            if description is not None:
                rule.description = description
            if rule_config is not None:
                rule.rule_config = rule_config
            if severity is not None:
                rule.severity = severity
            if enabled is not None:
                rule.enabled = enabled

            rule.updated_at = utc_now()
            session.flush()
            session.expunge(rule)
            return rule

    def delete_rule(self, rule_id: str) -> bool:
        """Delete a rule."""
        with get_session() as session:
            rule = session.query(ValidationRuleModel).filter(
                ValidationRuleModel.id == rule_id
            ).first()

            if not rule:
                return False

            session.delete(rule)
            return True

    # Validation Results

    def record_result(
        self,
        rule_id: str,
        status: str,
        rows_checked: int = 0,
        rows_passed: int = 0,
        rows_failed: int = 0,
        failure_samples: Optional[list] = None,
        details: Optional[dict] = None,
        run_id: Optional[str] = None,
    ) -> ValidationResultModel:
        """Record a validation result."""
        with get_session() as session:
            result = ValidationResultModel(
                id=str(uuid.uuid4()),
                rule_id=rule_id,
                run_id=run_id,
                status=status,
                rows_checked=rows_checked,
                rows_passed=rows_passed,
                rows_failed=rows_failed,
                failure_samples=failure_samples or [],
                details=details or {},
            )
            session.add(result)

            # Update rule status
            rule = session.query(ValidationRuleModel).filter(
                ValidationRuleModel.id == rule_id
            ).first()
            if rule:
                rule.last_run_at = utc_now()
                rule.last_status = status

            session.flush()
            session.expunge(result)
            return result

    def get_results(
        self,
        rule_id: Optional[str] = None,
        run_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> List[ValidationResultModel]:
        """Get validation results with optional filters."""
        with get_session() as session:
            query = session.query(ValidationResultModel)

            if rule_id:
                query = query.filter(ValidationResultModel.rule_id == rule_id)
            if run_id:
                query = query.filter(ValidationResultModel.run_id == run_id)
            if status:
                query = query.filter(ValidationResultModel.status == status)

            results = query.order_by(
                ValidationResultModel.executed_at.desc()
            ).limit(limit).all()

            for r in results:
                session.expunge(r)
            return results

    def get_latest_results_by_table(
        self,
        table_name: str,
    ) -> List[dict]:
        """Get latest validation results for all rules of a table."""
        with get_session() as session:
            rules = session.query(ValidationRuleModel).filter(
                ValidationRuleModel.target_table == table_name,
            ).all()

            results = []
            for rule in rules:
                latest = session.query(ValidationResultModel).filter(
                    ValidationResultModel.rule_id == rule.id,
                ).order_by(
                    ValidationResultModel.executed_at.desc()
                ).first()

                results.append({
                    "rule_id": rule.id,
                    "rule_name": rule.name,
                    "rule_type": rule.rule_type,
                    "severity": rule.severity,
                    "enabled": rule.enabled,
                    "last_status": rule.last_status,
                    "last_run_at": rule.last_run_at,
                    "latest_result": {
                        "status": latest.status,
                        "rows_checked": latest.rows_checked,
                        "rows_failed": latest.rows_failed,
                        "executed_at": latest.executed_at,
                    } if latest else None,
                })

            return results

    def get_failing_rules(self) -> List[ValidationRuleModel]:
        """Get all rules that are currently failing."""
        with get_session() as session:
            rules = session.query(ValidationRuleModel).filter(
                ValidationRuleModel.enabled == True,
                ValidationRuleModel.last_status == "failed",
            ).order_by(
                ValidationRuleModel.severity.asc()  # critical first
            ).all()

            for r in rules:
                session.expunge(r)
            return rules

    def get_quality_summary(self) -> dict:
        """Get a summary of data quality metrics."""
        with get_session() as session:
            total_rules = session.query(ValidationRuleModel).count()
            enabled_rules = session.query(ValidationRuleModel).filter(
                ValidationRuleModel.enabled == True
            ).count()
            passing_rules = session.query(ValidationRuleModel).filter(
                ValidationRuleModel.enabled == True,
                ValidationRuleModel.last_status == "passed",
            ).count()
            failing_rules = session.query(ValidationRuleModel).filter(
                ValidationRuleModel.enabled == True,
                ValidationRuleModel.last_status == "failed",
            ).count()

            # Count by severity
            critical_failing = session.query(ValidationRuleModel).filter(
                ValidationRuleModel.enabled == True,
                ValidationRuleModel.last_status == "failed",
                ValidationRuleModel.severity == "critical",
            ).count()

            return {
                "total_rules": total_rules,
                "enabled_rules": enabled_rules,
                "passing_rules": passing_rules,
                "failing_rules": failing_rules,
                "critical_failing": critical_failing,
                "pass_rate": passing_rules / enabled_rules * 100 if enabled_rules > 0 else 0,
            }


# Singleton instance
_validation_service: Optional[ValidationService] = None


def get_validation_service() -> ValidationService:
    """Get the validation service singleton."""
    global _validation_service
    if _validation_service is None:
        _validation_service = ValidationService()
    return _validation_service
