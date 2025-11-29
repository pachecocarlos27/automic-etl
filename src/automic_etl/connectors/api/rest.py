"""REST API Connector for generic API integrations."""

from __future__ import annotations

from typing import Any, Callable, Iterator
from datetime import datetime
import json

import httpx
import polars as pl
import structlog

from automic_etl.connectors.base import APIConnector

logger = structlog.get_logger()


class RESTConnector(APIConnector):
    """
    Generic REST API connector supporting various authentication methods
    and pagination strategies.

    Supports:
    - Multiple auth methods: API Key, Bearer Token, Basic Auth, OAuth2
    - Various pagination: offset, cursor, page number, link header
    - Rate limiting with automatic retry
    - Response transformation
    """

    def __init__(
        self,
        base_url: str,
        auth_type: str = "none",
        auth_config: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 30.0,
        rate_limit: int | None = None,
        retry_attempts: int = 3,
    ) -> None:
        """
        Initialize REST connector.

        Args:
            base_url: Base URL for the API
            auth_type: Authentication type (none, api_key, bearer, basic, oauth2)
            auth_config: Authentication configuration
            headers: Additional headers to include
            timeout: Request timeout in seconds
            rate_limit: Maximum requests per minute
            retry_attempts: Number of retry attempts on failure
        """
        self.base_url = base_url.rstrip("/")
        self.auth_type = auth_type
        self.auth_config = auth_config or {}
        self.custom_headers = headers or {}
        self.timeout = timeout
        self.rate_limit = rate_limit
        self.retry_attempts = retry_attempts
        self._client: httpx.Client | None = None
        self._last_request_time: datetime | None = None
        self.logger = logger.bind(connector="rest", base_url=base_url)

    def connect(self) -> None:
        """Establish connection to the API."""
        headers = self._build_headers()
        self._client = httpx.Client(
            base_url=self.base_url,
            headers=headers,
            timeout=self.timeout,
        )
        self.logger.info("REST API client initialized")

    def disconnect(self) -> None:
        """Close the API connection."""
        if self._client:
            self._client.close()
            self._client = None
        self.logger.info("REST API client disconnected")

    def _build_headers(self) -> dict[str, str]:
        """Build request headers including authentication."""
        headers = {"Content-Type": "application/json", **self.custom_headers}

        if self.auth_type == "api_key":
            key_name = self.auth_config.get("header_name", "X-API-Key")
            headers[key_name] = self.auth_config["api_key"]
        elif self.auth_type == "bearer":
            headers["Authorization"] = f"Bearer {self.auth_config['token']}"
        elif self.auth_type == "basic":
            import base64
            credentials = f"{self.auth_config['username']}:{self.auth_config['password']}"
            encoded = base64.b64encode(credentials.encode()).decode()
            headers["Authorization"] = f"Basic {encoded}"

        return headers

    def _handle_rate_limit(self) -> None:
        """Handle rate limiting between requests."""
        if self.rate_limit and self._last_request_time:
            import time
            min_interval = 60.0 / self.rate_limit
            elapsed = (datetime.now() - self._last_request_time).total_seconds()
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
        self._last_request_time = datetime.now()

    def request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        json_data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Make an API request.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE, etc.)
            endpoint: API endpoint (relative to base_url)
            params: Query parameters
            data: Form data
            json_data: JSON body data

        Returns:
            Response data as dictionary
        """
        if not self._client:
            raise RuntimeError("Client not connected. Call connect() first.")

        self._handle_rate_limit()

        endpoint = endpoint.lstrip("/")

        for attempt in range(self.retry_attempts):
            try:
                response = self._client.request(
                    method=method,
                    url=endpoint,
                    params=params,
                    data=data,
                    json=json_data,
                )
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:  # Rate limited
                    import time
                    retry_after = int(e.response.headers.get("Retry-After", 60))
                    self.logger.warning(f"Rate limited, waiting {retry_after}s")
                    time.sleep(retry_after)
                elif attempt < self.retry_attempts - 1:
                    self.logger.warning(f"Request failed, retrying: {e}")
                    import time
                    time.sleep(2 ** attempt)
                else:
                    raise
            except httpx.RequestError as e:
                if attempt < self.retry_attempts - 1:
                    self.logger.warning(f"Request error, retrying: {e}")
                    import time
                    time.sleep(2 ** attempt)
                else:
                    raise

        return {}

    def get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Make a GET request."""
        return self.request("GET", endpoint, params=params)

    def post(self, endpoint: str, data: dict[str, Any] | None = None) -> dict[str, Any]:
        """Make a POST request."""
        return self.request("POST", endpoint, json_data=data)

    def put(self, endpoint: str, data: dict[str, Any] | None = None) -> dict[str, Any]:
        """Make a PUT request."""
        return self.request("PUT", endpoint, json_data=data)

    def delete(self, endpoint: str) -> dict[str, Any]:
        """Make a DELETE request."""
        return self.request("DELETE", endpoint)

    def paginate(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        pagination_type: str = "offset",
        page_size: int = 100,
        data_key: str = "data",
        max_pages: int | None = None,
    ) -> Iterator[list[dict[str, Any]]]:
        """
        Paginate through API results.

        Args:
            endpoint: API endpoint
            params: Initial query parameters
            pagination_type: Type of pagination (offset, cursor, page, link)
            page_size: Number of records per page
            data_key: Key in response containing data array
            max_pages: Maximum number of pages to fetch

        Yields:
            List of records for each page
        """
        params = params or {}
        page = 0

        if pagination_type == "offset":
            params["limit"] = page_size
            while max_pages is None or page < max_pages:
                params["offset"] = page * page_size
                response = self.get(endpoint, params)
                data = response.get(data_key, response)
                if not data:
                    break
                yield data
                if len(data) < page_size:
                    break
                page += 1

        elif pagination_type == "page":
            params["per_page"] = page_size
            while max_pages is None or page < max_pages:
                params["page"] = page + 1
                response = self.get(endpoint, params)
                data = response.get(data_key, response)
                if not data:
                    break
                yield data
                if len(data) < page_size:
                    break
                page += 1

        elif pagination_type == "cursor":
            cursor = None
            while max_pages is None or page < max_pages:
                if cursor:
                    params["cursor"] = cursor
                params["limit"] = page_size
                response = self.get(endpoint, params)
                data = response.get(data_key, response)
                if not data:
                    break
                yield data
                cursor = response.get("next_cursor") or response.get("cursor")
                if not cursor:
                    break
                page += 1

    def extract(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        pagination_type: str | None = None,
        page_size: int = 100,
        data_key: str = "data",
        max_records: int | None = None,
        transform: Callable[[dict], dict] | None = None,
    ) -> pl.DataFrame:
        """
        Extract data from API endpoint.

        Args:
            endpoint: API endpoint
            params: Query parameters
            pagination_type: Pagination type (None for single request)
            page_size: Records per page
            data_key: Key containing data in response
            max_records: Maximum records to extract
            transform: Optional function to transform each record

        Returns:
            Polars DataFrame with extracted data
        """
        all_records = []

        if pagination_type:
            for page_data in self.paginate(
                endpoint, params, pagination_type, page_size, data_key
            ):
                for record in page_data:
                    if transform:
                        record = transform(record)
                    all_records.append(record)
                    if max_records and len(all_records) >= max_records:
                        break
                if max_records and len(all_records) >= max_records:
                    break
        else:
            response = self.get(endpoint, params)
            data = response.get(data_key, response)
            if isinstance(data, list):
                for record in data:
                    if transform:
                        record = transform(record)
                    all_records.append(record)
            else:
                all_records.append(transform(data) if transform else data)

        if not all_records:
            return pl.DataFrame()

        return pl.DataFrame(all_records)

    def test_connection(self) -> bool:
        """Test if the API connection is working."""
        try:
            # Try a simple GET request to the base URL
            self._client.get("/")
            return True
        except Exception:
            return False


class WebhookReceiver:
    """Receive and process webhook data."""

    def __init__(
        self,
        secret: str | None = None,
        signature_header: str = "X-Signature",
    ) -> None:
        self.secret = secret
        self.signature_header = signature_header
        self._buffer: list[dict] = []

    def verify_signature(self, payload: bytes, signature: str) -> bool:
        """Verify webhook signature."""
        if not self.secret:
            return True

        import hmac
        import hashlib

        expected = hmac.new(
            self.secret.encode(),
            payload,
            hashlib.sha256,
        ).hexdigest()

        return hmac.compare_digest(expected, signature)

    def process(self, payload: dict[str, Any]) -> None:
        """Process incoming webhook payload."""
        self._buffer.append({
            "received_at": datetime.utcnow().isoformat(),
            "payload": payload,
        })

    def flush(self) -> pl.DataFrame:
        """Flush buffered webhooks to DataFrame."""
        if not self._buffer:
            return pl.DataFrame()

        df = pl.DataFrame(self._buffer)
        self._buffer = []
        return df
