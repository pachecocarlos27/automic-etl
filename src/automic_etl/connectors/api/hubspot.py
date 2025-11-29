"""HubSpot connector for marketing and CRM data."""

from __future__ import annotations

from typing import Any, Iterator
from datetime import datetime

import polars as pl
import structlog

from automic_etl.connectors.base import APIConnector

logger = structlog.get_logger()


class HubSpotConnector(APIConnector):
    """
    HubSpot connector for contacts, companies, deals, and marketing data.

    Features:
    - Full object extraction (contacts, companies, deals, tickets)
    - Property-based filtering
    - Association extraction
    - Incremental sync support
    - Marketing data (emails, forms, campaigns)
    """

    BASE_URL = "https://api.hubapi.com"

    def __init__(
        self,
        access_token: str | None = None,
        api_key: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        refresh_token: str | None = None,
    ) -> None:
        """
        Initialize HubSpot connector.

        Args:
            access_token: OAuth access token (preferred)
            api_key: API key (deprecated but still supported)
            client_id: OAuth client ID
            client_secret: OAuth client secret
            refresh_token: OAuth refresh token
        """
        self.access_token = access_token
        self.api_key = api_key
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self._session = None
        self.logger = logger.bind(connector="hubspot")

    def connect(self) -> None:
        """Establish connection to HubSpot."""
        import httpx

        self._session = httpx.Client(
            base_url=self.BASE_URL,
            timeout=30.0,
        )

        if not self.access_token and self.refresh_token:
            self._refresh_access_token()

        self.logger.info("Connected to HubSpot")

    def _refresh_access_token(self) -> None:
        """Refresh OAuth access token."""
        response = self._session.post(
            "/oauth/v1/token",
            data={
                "grant_type": "refresh_token",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": self.refresh_token,
            },
        )
        response.raise_for_status()
        data = response.json()
        self.access_token = data["access_token"]

    def disconnect(self) -> None:
        """Close HubSpot connection."""
        if self._session:
            self._session.close()
            self._session = None

    def _get_headers(self) -> dict[str, str]:
        """Get request headers."""
        headers = {"Content-Type": "application/json"}
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"
        return headers

    def _api_request(
        self,
        method: str,
        endpoint: str,
        params: dict | None = None,
        data: dict | None = None,
    ) -> dict[str, Any]:
        """Make an API request."""
        # Add API key if using legacy auth
        if self.api_key and not self.access_token:
            params = params or {}
            params["hapikey"] = self.api_key

        response = self._session.request(
            method=method,
            url=endpoint,
            headers=self._get_headers(),
            params=params,
            json=data,
        )
        response.raise_for_status()
        return response.json() if response.content else {}

    def get_contacts(
        self,
        properties: list[str] | None = None,
        limit: int | None = None,
        after: str | None = None,
    ) -> pl.DataFrame:
        """
        Extract all contacts.

        Args:
            properties: Contact properties to retrieve
            limit: Maximum contacts to return
            after: Pagination cursor

        Returns:
            DataFrame with contacts
        """
        return self._get_crm_objects("contacts", properties, limit, after)

    def get_companies(
        self,
        properties: list[str] | None = None,
        limit: int | None = None,
    ) -> pl.DataFrame:
        """Extract all companies."""
        return self._get_crm_objects("companies", properties, limit)

    def get_deals(
        self,
        properties: list[str] | None = None,
        limit: int | None = None,
    ) -> pl.DataFrame:
        """Extract all deals."""
        return self._get_crm_objects("deals", properties, limit)

    def get_tickets(
        self,
        properties: list[str] | None = None,
        limit: int | None = None,
    ) -> pl.DataFrame:
        """Extract all tickets."""
        return self._get_crm_objects("tickets", properties, limit)

    def _get_crm_objects(
        self,
        object_type: str,
        properties: list[str] | None = None,
        limit: int | None = None,
        after: str | None = None,
    ) -> pl.DataFrame:
        """Generic CRM object extraction."""
        endpoint = f"/crm/v3/objects/{object_type}"
        all_records = []
        page_size = min(100, limit) if limit else 100

        while True:
            params = {"limit": page_size}
            if properties:
                params["properties"] = ",".join(properties)
            if after:
                params["after"] = after

            response = self._api_request("GET", endpoint, params)
            results = response.get("results", [])

            for record in results:
                flat_record = {"id": record["id"]}
                flat_record.update(record.get("properties", {}))
                all_records.append(flat_record)

            if limit and len(all_records) >= limit:
                all_records = all_records[:limit]
                break

            paging = response.get("paging", {})
            after = paging.get("next", {}).get("after")
            if not after:
                break

        return pl.DataFrame(all_records) if all_records else pl.DataFrame()

    def search_contacts(
        self,
        filters: list[dict[str, Any]],
        properties: list[str] | None = None,
        limit: int = 100,
    ) -> pl.DataFrame:
        """
        Search contacts with filters.

        Args:
            filters: List of filter objects
            properties: Properties to return
            limit: Maximum results

        Returns:
            DataFrame with matching contacts
        """
        return self._search_objects("contacts", filters, properties, limit)

    def _search_objects(
        self,
        object_type: str,
        filters: list[dict[str, Any]],
        properties: list[str] | None = None,
        limit: int = 100,
    ) -> pl.DataFrame:
        """Search CRM objects with filters."""
        endpoint = f"/crm/v3/objects/{object_type}/search"

        body = {
            "filterGroups": [{"filters": filters}],
            "limit": min(limit, 100),
        }
        if properties:
            body["properties"] = properties

        all_records = []
        after = 0

        while len(all_records) < limit:
            body["after"] = after
            response = self._api_request("POST", endpoint, data=body)
            results = response.get("results", [])

            for record in results:
                flat_record = {"id": record["id"]}
                flat_record.update(record.get("properties", {}))
                all_records.append(flat_record)

            if not response.get("paging"):
                break
            after = response["paging"]["next"]["after"]

        return pl.DataFrame(all_records[:limit]) if all_records else pl.DataFrame()

    def get_contact_lists(self) -> pl.DataFrame:
        """Get all contact lists."""
        endpoint = "/contacts/v1/lists"
        response = self._api_request("GET", endpoint)

        lists = []
        for lst in response.get("lists", []):
            lists.append({
                "list_id": lst["listId"],
                "name": lst["name"],
                "dynamic": lst["dynamic"],
                "size": lst.get("metaData", {}).get("size", 0),
                "created_at": lst.get("createdAt"),
                "updated_at": lst.get("updatedAt"),
            })

        return pl.DataFrame(lists) if lists else pl.DataFrame()

    def get_list_contacts(
        self,
        list_id: int,
        properties: list[str] | None = None,
    ) -> pl.DataFrame:
        """Get contacts in a specific list."""
        endpoint = f"/contacts/v1/lists/{list_id}/contacts/all"
        all_contacts = []
        vid_offset = 0

        while True:
            params = {"count": 100, "vidOffset": vid_offset}
            if properties:
                params["property"] = properties

            response = self._api_request("GET", endpoint, params)
            contacts = response.get("contacts", [])

            for contact in contacts:
                flat_contact = {"vid": contact["vid"]}
                for prop, data in contact.get("properties", {}).items():
                    flat_contact[prop] = data.get("value")
                all_contacts.append(flat_contact)

            if not response.get("has-more"):
                break
            vid_offset = response["vid-offset"]

        return pl.DataFrame(all_contacts) if all_contacts else pl.DataFrame()

    def get_email_events(
        self,
        event_type: str | None = None,
        since: datetime | None = None,
        limit: int = 1000,
    ) -> pl.DataFrame:
        """
        Get email events (opens, clicks, bounces, etc.).

        Args:
            event_type: Filter by event type (OPEN, CLICK, BOUNCE, etc.)
            since: Only events after this time
            limit: Maximum events to return

        Returns:
            DataFrame with email events
        """
        endpoint = "/email/public/v1/events"
        all_events = []
        offset = None

        while len(all_events) < limit:
            params = {"limit": min(1000, limit - len(all_events))}
            if event_type:
                params["eventType"] = event_type
            if since:
                params["startTimestamp"] = int(since.timestamp() * 1000)
            if offset:
                params["offset"] = offset

            response = self._api_request("GET", endpoint, params)
            events = response.get("events", [])
            all_events.extend(events)

            if not response.get("hasMore"):
                break
            offset = response.get("offset")

        return pl.DataFrame(all_events[:limit]) if all_events else pl.DataFrame()

    def get_forms(self) -> pl.DataFrame:
        """Get all forms."""
        endpoint = "/forms/v2/forms"
        response = self._api_request("GET", endpoint)

        forms = []
        for form in response:
            forms.append({
                "guid": form["guid"],
                "name": form["name"],
                "action": form.get("action"),
                "created_at": form.get("createdAt"),
                "updated_at": form.get("updatedAt"),
                "submissions_count": form.get("submissionCount", 0),
            })

        return pl.DataFrame(forms) if forms else pl.DataFrame()

    def get_form_submissions(
        self,
        form_id: str,
        limit: int = 1000,
    ) -> pl.DataFrame:
        """Get submissions for a specific form."""
        endpoint = f"/form-integrations/v1/submissions/forms/{form_id}"
        all_submissions = []
        after = None

        while len(all_submissions) < limit:
            params = {"limit": min(50, limit - len(all_submissions))}
            if after:
                params["after"] = after

            response = self._api_request("GET", endpoint, params)
            results = response.get("results", [])

            for sub in results:
                flat_sub = {
                    "submission_id": sub.get("submittedAt"),
                    "page_url": sub.get("pageUrl"),
                }
                for value in sub.get("values", []):
                    flat_sub[value["name"]] = value.get("value")
                all_submissions.append(flat_sub)

            paging = response.get("paging", {})
            after = paging.get("next", {}).get("after")
            if not after:
                break

        return pl.DataFrame(all_submissions[:limit]) if all_submissions else pl.DataFrame()

    def get_pipelines(self, object_type: str = "deals") -> pl.DataFrame:
        """Get pipelines for deals or tickets."""
        endpoint = f"/crm/v3/pipelines/{object_type}"
        response = self._api_request("GET", endpoint)

        pipelines = []
        for pipeline in response.get("results", []):
            for stage in pipeline.get("stages", []):
                pipelines.append({
                    "pipeline_id": pipeline["id"],
                    "pipeline_label": pipeline["label"],
                    "stage_id": stage["id"],
                    "stage_label": stage["label"],
                    "display_order": stage["displayOrder"],
                })

        return pl.DataFrame(pipelines) if pipelines else pl.DataFrame()

    def get_owners(self) -> pl.DataFrame:
        """Get all HubSpot owners/users."""
        endpoint = "/crm/v3/owners"
        response = self._api_request("GET", endpoint)

        owners = []
        for owner in response.get("results", []):
            owners.append({
                "id": owner["id"],
                "email": owner.get("email"),
                "first_name": owner.get("firstName"),
                "last_name": owner.get("lastName"),
                "user_id": owner.get("userId"),
                "created_at": owner.get("createdAt"),
                "updated_at": owner.get("updatedAt"),
            })

        return pl.DataFrame(owners) if owners else pl.DataFrame()

    def create_contact(self, properties: dict[str, Any]) -> str:
        """Create a new contact."""
        endpoint = "/crm/v3/objects/contacts"
        response = self._api_request("POST", endpoint, data={"properties": properties})
        return response["id"]

    def update_contact(self, contact_id: str, properties: dict[str, Any]) -> None:
        """Update a contact."""
        endpoint = f"/crm/v3/objects/contacts/{contact_id}"
        self._api_request("PATCH", endpoint, data={"properties": properties})

    def test_connection(self) -> bool:
        """Test the HubSpot connection."""
        try:
            self._api_request("GET", "/crm/v3/objects/contacts", {"limit": 1})
            return True
        except Exception:
            return False
