"""Salesforce connector for CRM data integration."""

from __future__ import annotations

from typing import Any, Iterator
from datetime import datetime, timedelta

import polars as pl
import structlog

from automic_etl.connectors.base import APIConnector

logger = structlog.get_logger()


class SalesforceConnector(APIConnector):
    """
    Salesforce CRM connector supporting bulk and REST APIs.

    Features:
    - OAuth2 and username/password authentication
    - Bulk API for large data volumes
    - REST API for real-time queries
    - SOQL query support
    - Change Data Capture (CDC) for incremental loads
    """

    BULK_API_VERSION = "v58.0"
    REST_API_VERSION = "v58.0"

    def __init__(
        self,
        instance_url: str | None = None,
        username: str | None = None,
        password: str | None = None,
        security_token: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        access_token: str | None = None,
        refresh_token: str | None = None,
        sandbox: bool = False,
    ) -> None:
        """
        Initialize Salesforce connector.

        For username/password auth: provide username, password, security_token
        For OAuth2: provide client_id, client_secret, and optionally refresh_token
        For existing session: provide instance_url and access_token
        """
        self.instance_url = instance_url
        self.username = username
        self.password = password
        self.security_token = security_token
        self.client_id = client_id
        self.client_secret = client_secret
        self._access_token = access_token
        self._refresh_token = refresh_token
        self.sandbox = sandbox

        self._session = None
        self._token_expires_at: datetime | None = None
        self.logger = logger.bind(connector="salesforce")

    def connect(self) -> None:
        """Authenticate and establish connection to Salesforce."""
        import httpx

        self._session = httpx.Client(timeout=30.0)

        if self._access_token and self.instance_url:
            # Use existing token
            self.logger.info("Using existing access token")
            return

        login_url = (
            "https://test.salesforce.com" if self.sandbox
            else "https://login.salesforce.com"
        )

        if self.username and self.password:
            # Username/password authentication
            self._authenticate_password(login_url)
        elif self.client_id and self.client_secret:
            # OAuth2 authentication
            self._authenticate_oauth(login_url)
        else:
            raise ValueError("No valid authentication credentials provided")

        self.logger.info("Connected to Salesforce", instance=self.instance_url)

    def _authenticate_password(self, login_url: str) -> None:
        """Authenticate using username and password."""
        auth_url = f"{login_url}/services/oauth2/token"

        payload = {
            "grant_type": "password",
            "client_id": self.client_id or "3MVG9...",  # Default connected app
            "client_secret": self.client_secret or "",
            "username": self.username,
            "password": f"{self.password}{self.security_token or ''}",
        }

        response = self._session.post(auth_url, data=payload)
        response.raise_for_status()

        data = response.json()
        self._access_token = data["access_token"]
        self.instance_url = data["instance_url"]

    def _authenticate_oauth(self, login_url: str) -> None:
        """Authenticate using OAuth2."""
        auth_url = f"{login_url}/services/oauth2/token"

        if self._refresh_token:
            payload = {
                "grant_type": "refresh_token",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": self._refresh_token,
            }
        else:
            raise ValueError("OAuth2 requires refresh_token for server-to-server auth")

        response = self._session.post(auth_url, data=payload)
        response.raise_for_status()

        data = response.json()
        self._access_token = data["access_token"]
        self.instance_url = data.get("instance_url", self.instance_url)
        self._token_expires_at = datetime.utcnow() + timedelta(hours=2)

    def disconnect(self) -> None:
        """Close Salesforce connection."""
        if self._session:
            self._session.close()
            self._session = None
        self.logger.info("Disconnected from Salesforce")

    def _get_headers(self) -> dict[str, str]:
        """Get request headers with authentication."""
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json",
        }

    def _api_request(
        self,
        method: str,
        endpoint: str,
        data: dict | None = None,
    ) -> dict[str, Any]:
        """Make an API request to Salesforce."""
        url = f"{self.instance_url}{endpoint}"
        response = self._session.request(
            method=method,
            url=url,
            headers=self._get_headers(),
            json=data,
        )
        response.raise_for_status()
        return response.json() if response.content else {}

    def query(self, soql: str) -> pl.DataFrame:
        """
        Execute a SOQL query.

        Args:
            soql: SOQL query string

        Returns:
            DataFrame with query results
        """
        endpoint = f"/services/data/{self.REST_API_VERSION}/query"
        records = []

        # Initial query
        response = self._api_request("GET", f"{endpoint}?q={soql}")
        records.extend(response.get("records", []))

        # Handle pagination
        while not response.get("done", True):
            next_url = response["nextRecordsUrl"]
            response = self._api_request("GET", next_url)
            records.extend(response.get("records", []))

        # Clean up records (remove attributes)
        cleaned = []
        for record in records:
            clean_record = {
                k: v for k, v in record.items()
                if k != "attributes"
            }
            cleaned.append(clean_record)

        if not cleaned:
            return pl.DataFrame()

        return pl.DataFrame(cleaned)

    def query_all(self, soql: str) -> pl.DataFrame:
        """
        Execute SOQL query including deleted and archived records.

        Args:
            soql: SOQL query string

        Returns:
            DataFrame with all matching records
        """
        endpoint = f"/services/data/{self.REST_API_VERSION}/queryAll"
        records = []

        response = self._api_request("GET", f"{endpoint}?q={soql}")
        records.extend(response.get("records", []))

        while not response.get("done", True):
            next_url = response["nextRecordsUrl"]
            response = self._api_request("GET", next_url)
            records.extend(response.get("records", []))

        cleaned = [
            {k: v for k, v in r.items() if k != "attributes"}
            for r in records
        ]

        return pl.DataFrame(cleaned) if cleaned else pl.DataFrame()

    def describe(self, sobject: str) -> dict[str, Any]:
        """
        Get metadata for a Salesforce object.

        Args:
            sobject: Salesforce object name (e.g., 'Account', 'Contact')

        Returns:
            Object metadata including fields
        """
        endpoint = f"/services/data/{self.REST_API_VERSION}/sobjects/{sobject}/describe"
        return self._api_request("GET", endpoint)

    def get_objects(self) -> list[str]:
        """Get list of available Salesforce objects."""
        endpoint = f"/services/data/{self.REST_API_VERSION}/sobjects"
        response = self._api_request("GET", endpoint)
        return [obj["name"] for obj in response.get("sobjects", [])]

    def extract_object(
        self,
        sobject: str,
        fields: list[str] | None = None,
        where: str | None = None,
        limit: int | None = None,
    ) -> pl.DataFrame:
        """
        Extract all records from a Salesforce object.

        Args:
            sobject: Salesforce object name
            fields: List of fields to extract (None for all)
            where: Optional WHERE clause
            limit: Maximum records to return

        Returns:
            DataFrame with extracted records
        """
        if not fields:
            # Get all fields from describe
            metadata = self.describe(sobject)
            fields = [f["name"] for f in metadata["fields"]]

        soql = f"SELECT {', '.join(fields)} FROM {sobject}"
        if where:
            soql += f" WHERE {where}"
        if limit:
            soql += f" LIMIT {limit}"

        return self.query(soql)

    def extract_incremental(
        self,
        sobject: str,
        fields: list[str] | None = None,
        since: datetime | None = None,
        timestamp_field: str = "LastModifiedDate",
    ) -> pl.DataFrame:
        """
        Extract records modified since a given timestamp.

        Args:
            sobject: Salesforce object name
            fields: Fields to extract
            since: Only records modified after this time
            timestamp_field: Field to use for filtering

        Returns:
            DataFrame with modified records
        """
        where = None
        if since:
            since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ")
            where = f"{timestamp_field} > {since_str}"

        return self.extract_object(sobject, fields, where)

    def bulk_query(self, soql: str, timeout: int = 600) -> pl.DataFrame:
        """
        Execute a bulk query for large datasets.

        Args:
            soql: SOQL query
            timeout: Maximum wait time in seconds

        Returns:
            DataFrame with query results
        """
        import time

        # Create bulk job
        job_endpoint = f"/services/data/{self.BULK_API_VERSION}/jobs/query"
        job_data = {
            "operation": "query",
            "query": soql,
        }
        job = self._api_request("POST", job_endpoint, job_data)
        job_id = job["id"]

        self.logger.info("Bulk query job created", job_id=job_id)

        # Poll for completion
        start_time = time.time()
        while time.time() - start_time < timeout:
            status = self._api_request("GET", f"{job_endpoint}/{job_id}")
            state = status["state"]

            if state == "JobComplete":
                break
            elif state in ("Failed", "Aborted"):
                raise RuntimeError(f"Bulk query failed: {status.get('errorMessage')}")

            time.sleep(5)

        # Get results
        results_endpoint = f"{job_endpoint}/{job_id}/results"
        response = self._session.get(
            f"{self.instance_url}{results_endpoint}",
            headers={**self._get_headers(), "Accept": "text/csv"},
        )
        response.raise_for_status()

        # Parse CSV response
        from io import StringIO
        return pl.read_csv(StringIO(response.text))

    def create_record(self, sobject: str, data: dict[str, Any]) -> str:
        """Create a new record."""
        endpoint = f"/services/data/{self.REST_API_VERSION}/sobjects/{sobject}"
        response = self._api_request("POST", endpoint, data)
        return response["id"]

    def update_record(self, sobject: str, record_id: str, data: dict[str, Any]) -> None:
        """Update an existing record."""
        endpoint = f"/services/data/{self.REST_API_VERSION}/sobjects/{sobject}/{record_id}"
        self._session.patch(
            f"{self.instance_url}{endpoint}",
            headers=self._get_headers(),
            json=data,
        )

    def delete_record(self, sobject: str, record_id: str) -> None:
        """Delete a record."""
        endpoint = f"/services/data/{self.REST_API_VERSION}/sobjects/{sobject}/{record_id}"
        self._session.delete(
            f"{self.instance_url}{endpoint}",
            headers=self._get_headers(),
        )

    def upsert(
        self,
        sobject: str,
        external_id_field: str,
        records: list[dict[str, Any]],
    ) -> dict[str, int]:
        """
        Upsert records using an external ID field.

        Args:
            sobject: Salesforce object
            external_id_field: External ID field name
            records: Records to upsert

        Returns:
            Counts of created, updated, and failed records
        """
        endpoint = f"/services/data/{self.BULK_API_VERSION}/jobs/ingest"

        # Create job
        job_data = {
            "operation": "upsert",
            "object": sobject,
            "externalIdFieldName": external_id_field,
            "contentType": "JSON",
        }
        job = self._api_request("POST", endpoint, job_data)
        job_id = job["id"]

        # Upload data
        upload_endpoint = f"{endpoint}/{job_id}/batches"
        for i in range(0, len(records), 10000):
            batch = records[i:i + 10000]
            self._api_request("PUT", upload_endpoint, {"records": batch})

        # Close job
        self._api_request("PATCH", f"{endpoint}/{job_id}", {"state": "UploadComplete"})

        # Wait for completion and get results
        import time
        while True:
            status = self._api_request("GET", f"{endpoint}/{job_id}")
            if status["state"] in ("JobComplete", "Failed", "Aborted"):
                return {
                    "created": status.get("numberRecordsProcessed", 0),
                    "failed": status.get("numberRecordsFailed", 0),
                }
            time.sleep(5)

    def test_connection(self) -> bool:
        """Test the Salesforce connection."""
        try:
            self._api_request("GET", f"/services/data/{self.REST_API_VERSION}/limits")
            return True
        except Exception:
            return False
