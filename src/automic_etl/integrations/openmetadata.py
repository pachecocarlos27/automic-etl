"""OpenMetadata integration for data cataloging and governance."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Literal
from datetime import datetime
import json

logger = logging.getLogger(__name__)


@dataclass
class OpenMetadataConfig:
    """OpenMetadata configuration."""

    server_url: str = "http://localhost:8585"
    api_version: str = "v1"

    # Authentication
    auth_provider: Literal["no-auth", "basic", "google", "okta", "azure", "custom-oidc"] = "no-auth"
    username: str | None = None
    password: str | None = None
    secret_key: str | None = None
    token: str | None = None

    # Default settings
    service_name: str = "automic_etl"
    database_name: str = "default"
    schema_name: str = "public"


class OpenMetadataIntegration:
    """
    OpenMetadata integration for data cataloging and governance.

    Features:
    - Asset registration (databases, tables, pipelines)
    - Metadata management
    - Data lineage
    - Data quality integration
    - Glossary and tags
    - Data profiling

    Example:
        config = OpenMetadataConfig(
            server_url="http://openmetadata.example.com:8585",
            auth_provider="basic",
            username="admin",
            password="admin"
        )

        om = OpenMetadataIntegration(config)

        # Register a table
        om.create_table(
            name="customers",
            columns=[
                {"name": "id", "dataType": "INT", "description": "Customer ID"},
                {"name": "name", "dataType": "STRING", "description": "Customer name"},
            ]
        )

        # Add lineage
        om.add_lineage(
            from_entity="bronze.raw_customers",
            to_entity="silver.customers",
            pipeline="customer_transform"
        )
    """

    def __init__(self, config: OpenMetadataConfig):
        self.config = config
        self._client = None
        self._session = None

    @property
    def api_url(self) -> str:
        """Get API base URL."""
        return f"{self.config.server_url}/api/{self.config.api_version}"

    def _ensure_session(self):
        """Ensure we have an authenticated session."""
        if self._session is None:
            import requests
            self._session = requests.Session()
            self._session.headers["Content-Type"] = "application/json"

            if self.config.auth_provider == "basic":
                self._session.auth = (self.config.username, self.config.password)
            elif self.config.token:
                self._session.headers["Authorization"] = f"Bearer {self.config.token}"

        return self._session

    def _request(
        self,
        method: str,
        endpoint: str,
        data: dict | None = None,
        params: dict | None = None,
    ) -> dict[str, Any]:
        """Make API request."""
        session = self._ensure_session()
        url = f"{self.api_url}/{endpoint}"

        response = session.request(
            method=method,
            url=url,
            json=data,
            params=params,
        )

        if response.status_code >= 400:
            logger.error(f"OpenMetadata API error: {response.status_code} - {response.text}")
            response.raise_for_status()

        return response.json() if response.text else {}

    # ========================
    # Database Service
    # ========================

    def create_database_service(
        self,
        name: str,
        service_type: str = "CustomDatabase",
        description: str = "",
        connection: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Create a database service.

        Args:
            name: Service name
            service_type: Type of database service
            description: Service description
            connection: Connection configuration

        Returns:
            Created service
        """
        data = {
            "name": name,
            "serviceType": service_type,
            "description": description,
            "connection": connection or {
                "config": {
                    "type": "CustomDatabase",
                    "sourcePythonClass": "automic_etl.connectors.base.BaseConnector"
                }
            }
        }

        return self._request("POST", "services/databaseServices", data=data)

    def get_database_service(self, name: str) -> dict[str, Any]:
        """Get a database service by name."""
        return self._request("GET", f"services/databaseServices/name/{name}")

    # ========================
    # Database
    # ========================

    def create_database(
        self,
        name: str,
        service_name: str | None = None,
        description: str = "",
    ) -> dict[str, Any]:
        """
        Create a database.

        Args:
            name: Database name
            service_name: Parent service name
            description: Database description

        Returns:
            Created database
        """
        service_name = service_name or self.config.service_name

        data = {
            "name": name,
            "service": service_name,
            "description": description,
        }

        return self._request("POST", "databases", data=data)

    def get_database(self, fqn: str) -> dict[str, Any]:
        """Get a database by fully qualified name."""
        return self._request("GET", f"databases/name/{fqn}")

    # ========================
    # Schema
    # ========================

    def create_schema(
        self,
        name: str,
        database_name: str | None = None,
        service_name: str | None = None,
        description: str = "",
    ) -> dict[str, Any]:
        """
        Create a database schema.

        Args:
            name: Schema name
            database_name: Parent database name
            service_name: Parent service name
            description: Schema description

        Returns:
            Created schema
        """
        service_name = service_name or self.config.service_name
        database_name = database_name or self.config.database_name

        data = {
            "name": name,
            "database": f"{service_name}.{database_name}",
            "description": description,
        }

        return self._request("POST", "databaseSchemas", data=data)

    # ========================
    # Tables
    # ========================

    def create_table(
        self,
        name: str,
        columns: list[dict[str, Any]],
        schema_name: str | None = None,
        database_name: str | None = None,
        service_name: str | None = None,
        description: str = "",
        table_type: str = "Regular",
        tags: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Create a table.

        Args:
            name: Table name
            columns: List of column definitions
            schema_name: Parent schema name
            database_name: Parent database name
            service_name: Parent service name
            description: Table description
            table_type: Table type (Regular, External, View, etc.)
            tags: Tags to apply

        Returns:
            Created table
        """
        service_name = service_name or self.config.service_name
        database_name = database_name or self.config.database_name
        schema_name = schema_name or self.config.schema_name

        # Build column definitions
        column_defs = []
        for col in columns:
            column_def = {
                "name": col["name"],
                "dataType": col.get("dataType", "STRING"),
                "description": col.get("description", ""),
            }
            if "constraint" in col:
                column_def["constraint"] = col["constraint"]
            if "dataLength" in col:
                column_def["dataLength"] = col["dataLength"]
            if "tags" in col:
                column_def["tags"] = col["tags"]
            column_defs.append(column_def)

        data = {
            "name": name,
            "databaseSchema": f"{service_name}.{database_name}.{schema_name}",
            "description": description,
            "tableType": table_type,
            "columns": column_defs,
        }

        if tags:
            data["tags"] = [{"tagFQN": tag} for tag in tags]

        return self._request("POST", "tables", data=data)

    def get_table(self, fqn: str) -> dict[str, Any]:
        """Get a table by fully qualified name."""
        return self._request("GET", f"tables/name/{fqn}")

    def update_table_description(
        self,
        fqn: str,
        description: str,
    ) -> dict[str, Any]:
        """Update a table's description."""
        data = [
            {
                "op": "add",
                "path": "/description",
                "value": description,
            }
        ]
        return self._request("PATCH", f"tables/name/{fqn}", data=data)

    def add_table_tags(
        self,
        fqn: str,
        tags: list[str],
    ) -> dict[str, Any]:
        """Add tags to a table."""
        tag_refs = [{"tagFQN": tag, "source": "Classification"} for tag in tags]
        data = [
            {
                "op": "add",
                "path": "/tags",
                "value": tag_refs,
            }
        ]
        return self._request("PATCH", f"tables/name/{fqn}", data=data)

    def list_tables(
        self,
        database: str | None = None,
        schema: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """List tables."""
        params = {"limit": limit}
        if database:
            params["database"] = database
        if schema:
            params["databaseSchema"] = schema

        response = self._request("GET", "tables", params=params)
        return response.get("data", [])

    # ========================
    # Lineage
    # ========================

    def add_lineage(
        self,
        from_entity: str,
        to_entity: str,
        from_type: str = "table",
        to_type: str = "table",
        pipeline: str | None = None,
        description: str = "",
    ) -> dict[str, Any]:
        """
        Add lineage between entities.

        Args:
            from_entity: Source entity FQN
            to_entity: Target entity FQN
            from_type: Source entity type
            to_type: Target entity type
            pipeline: Pipeline that created this lineage
            description: Lineage description

        Returns:
            Created lineage
        """
        data = {
            "edge": {
                "fromEntity": {
                    "id": self._get_entity_id(from_entity, from_type),
                    "type": from_type,
                },
                "toEntity": {
                    "id": self._get_entity_id(to_entity, to_type),
                    "type": to_type,
                },
                "description": description,
            }
        }

        if pipeline:
            data["edge"]["pipeline"] = {
                "id": self._get_entity_id(pipeline, "pipeline"),
                "type": "pipeline",
            }

        return self._request("PUT", "lineage", data=data)

    def _get_entity_id(self, fqn: str, entity_type: str) -> str:
        """Get entity ID from FQN."""
        endpoint_map = {
            "table": "tables",
            "pipeline": "pipelines",
            "dashboard": "dashboards",
            "topic": "topics",
        }
        endpoint = endpoint_map.get(entity_type, entity_type)
        entity = self._request("GET", f"{endpoint}/name/{fqn}")
        return entity.get("id")

    def get_lineage(
        self,
        fqn: str,
        entity_type: str = "table",
        depth: int = 1,
        direction: Literal["upstream", "downstream", "both"] = "both",
    ) -> dict[str, Any]:
        """
        Get lineage for an entity.

        Args:
            fqn: Entity fully qualified name
            entity_type: Entity type
            depth: How many levels to traverse
            direction: Direction to traverse

        Returns:
            Lineage graph
        """
        entity_id = self._get_entity_id(fqn, entity_type)
        params = {
            "upstreamDepth": depth if direction in ("upstream", "both") else 0,
            "downstreamDepth": depth if direction in ("downstream", "both") else 0,
        }
        return self._request(
            "GET",
            f"lineage/{entity_type}/name/{fqn}",
            params=params
        )

    # ========================
    # Pipelines
    # ========================

    def create_pipeline_service(
        self,
        name: str,
        service_type: str = "CustomPipeline",
        description: str = "",
    ) -> dict[str, Any]:
        """Create a pipeline service."""
        data = {
            "name": name,
            "serviceType": service_type,
            "description": description,
            "connection": {
                "config": {
                    "type": "CustomPipeline",
                }
            }
        }
        return self._request("POST", "services/pipelineServices", data=data)

    def create_pipeline(
        self,
        name: str,
        service_name: str | None = None,
        description: str = "",
        tasks: list[dict[str, Any]] | None = None,
        schedule: str | None = None,
        tags: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Create a pipeline.

        Args:
            name: Pipeline name
            service_name: Parent service name
            description: Pipeline description
            tasks: List of task definitions
            schedule: Cron schedule
            tags: Tags to apply

        Returns:
            Created pipeline
        """
        service_name = service_name or f"{self.config.service_name}_pipelines"

        data = {
            "name": name,
            "service": service_name,
            "description": description,
        }

        if tasks:
            data["tasks"] = tasks
        if schedule:
            data["scheduleInterval"] = schedule
        if tags:
            data["tags"] = [{"tagFQN": tag} for tag in tags]

        return self._request("POST", "pipelines", data=data)

    def update_pipeline_status(
        self,
        fqn: str,
        status: Literal["Successful", "Failed", "Pending", "Running"],
        run_id: str | None = None,
    ) -> dict[str, Any]:
        """Update pipeline status."""
        data = {
            "pipelineStatus": {
                "executionStatus": status,
                "timestamp": int(datetime.now().timestamp() * 1000),
            }
        }
        if run_id:
            data["pipelineStatus"]["runId"] = run_id

        return self._request("PUT", f"pipelines/name/{fqn}/status", data=data)

    # ========================
    # Glossary & Tags
    # ========================

    def create_glossary(
        self,
        name: str,
        description: str = "",
        owner: str | None = None,
    ) -> dict[str, Any]:
        """Create a glossary."""
        data = {
            "name": name,
            "description": description,
        }
        if owner:
            data["owner"] = {"id": owner, "type": "user"}

        return self._request("POST", "glossaries", data=data)

    def create_glossary_term(
        self,
        name: str,
        glossary_name: str,
        description: str = "",
        synonyms: list[str] | None = None,
        related_terms: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Create a glossary term.

        Args:
            name: Term name
            glossary_name: Parent glossary name
            description: Term description
            synonyms: List of synonyms
            related_terms: Related term FQNs

        Returns:
            Created term
        """
        data = {
            "name": name,
            "glossary": glossary_name,
            "description": description,
        }
        if synonyms:
            data["synonyms"] = synonyms
        if related_terms:
            data["relatedTerms"] = [{"id": t} for t in related_terms]

        return self._request("POST", "glossaryTerms", data=data)

    def create_classification(
        self,
        name: str,
        description: str = "",
    ) -> dict[str, Any]:
        """Create a classification (tag group)."""
        data = {
            "name": name,
            "description": description,
        }
        return self._request("POST", "classifications", data=data)

    def create_tag(
        self,
        name: str,
        classification_name: str,
        description: str = "",
    ) -> dict[str, Any]:
        """Create a tag under a classification."""
        data = {
            "name": name,
            "classification": classification_name,
            "description": description,
        }
        return self._request("POST", "tags", data=data)

    # ========================
    # Data Quality
    # ========================

    def add_test_suite(
        self,
        name: str,
        description: str = "",
    ) -> dict[str, Any]:
        """Create a test suite."""
        data = {
            "name": name,
            "description": description,
        }
        return self._request("POST", "dataQuality/testSuites", data=data)

    def add_test_case(
        self,
        name: str,
        test_suite: str,
        entity_fqn: str,
        test_definition: str,
        parameter_values: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Add a test case.

        Args:
            name: Test case name
            test_suite: Parent test suite name
            entity_fqn: Entity to test (table FQN)
            test_definition: Test definition name
            parameter_values: Test parameters

        Returns:
            Created test case
        """
        data = {
            "name": name,
            "testSuite": test_suite,
            "entityLink": f"<#E::table::{entity_fqn}>",
            "testDefinition": test_definition,
        }
        if parameter_values:
            data["parameterValues"] = [
                {"name": k, "value": str(v)}
                for k, v in parameter_values.items()
            ]

        return self._request("POST", "dataQuality/testCases", data=data)

    def add_test_result(
        self,
        test_case_fqn: str,
        status: Literal["Success", "Failed", "Aborted"],
        result: str = "",
        sample_data: list[dict] | None = None,
    ) -> dict[str, Any]:
        """
        Add a test result.

        Args:
            test_case_fqn: Test case FQN
            status: Test status
            result: Result message
            sample_data: Sample of failed data

        Returns:
            Created test result
        """
        data = {
            "testCaseResult": {
                "timestamp": int(datetime.now().timestamp() * 1000),
                "testCaseStatus": status,
                "result": result,
            }
        }
        if sample_data:
            data["testCaseResult"]["sampleData"] = sample_data

        return self._request(
            "PUT",
            f"dataQuality/testCases/name/{test_case_fqn}/testCaseResult",
            data=data
        )

    # ========================
    # Data Profiling
    # ========================

    def add_table_profile(
        self,
        table_fqn: str,
        profile_data: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Add profiling data for a table.

        Args:
            table_fqn: Table FQN
            profile_data: Profiling metrics

        Returns:
            Created profile
        """
        data = {
            "tableProfile": {
                "timestamp": int(datetime.now().timestamp() * 1000),
                "rowCount": profile_data.get("row_count"),
                "columnCount": profile_data.get("column_count"),
                "sizeInBytes": profile_data.get("size_bytes"),
            }
        }

        return self._request(
            "PUT",
            f"tables/name/{table_fqn}/tableProfile",
            data=data
        )

    def add_column_profile(
        self,
        table_fqn: str,
        column_name: str,
        profile_data: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Add profiling data for a column.

        Args:
            table_fqn: Table FQN
            column_name: Column name
            profile_data: Column profiling metrics

        Returns:
            Created profile
        """
        column_profile = {
            "timestamp": int(datetime.now().timestamp() * 1000),
            "name": column_name,
        }

        # Add available metrics
        metric_map = {
            "null_count": "nullCount",
            "null_proportion": "nullProportion",
            "unique_count": "uniqueCount",
            "unique_proportion": "uniqueProportion",
            "distinct_count": "distinctCount",
            "min": "min",
            "max": "max",
            "mean": "mean",
            "sum": "sum",
            "std_dev": "stddev",
            "median": "median",
            "min_length": "minLength",
            "max_length": "maxLength",
            "values_count": "valuesCount",
        }

        for key, api_key in metric_map.items():
            if key in profile_data:
                column_profile[api_key] = profile_data[key]

        data = {"columnProfile": column_profile}

        return self._request(
            "PUT",
            f"tables/name/{table_fqn}/columnProfile",
            data=data
        )

    # ========================
    # Search
    # ========================

    def search(
        self,
        query: str,
        index: str = "all",
        from_: int = 0,
        size: int = 10,
        filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Search for entities.

        Args:
            query: Search query
            index: Index to search (table, topic, dashboard, pipeline, all)
            from_: Starting offset
            size: Number of results
            filters: Additional filters

        Returns:
            Search results
        """
        params = {
            "q": query,
            "index": index,
            "from": from_,
            "size": size,
        }
        if filters:
            params.update(filters)

        return self._request("GET", "search/query", params=params)
