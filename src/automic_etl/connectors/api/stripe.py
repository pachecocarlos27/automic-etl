"""Stripe connector for payment and subscription data."""

from __future__ import annotations

from typing import Any, Iterator
from datetime import datetime

import polars as pl
import structlog

from automic_etl.connectors.base import APIConnector

logger = structlog.get_logger()


class StripeConnector(APIConnector):
    """
    Stripe connector for payment, subscription, and customer data.

    Features:
    - Customer and subscription extraction
    - Payment and invoice data
    - Charge and refund history
    - Product and price catalogs
    - Event/webhook data
    """

    BASE_URL = "https://api.stripe.com/v1"

    def __init__(
        self,
        api_key: str,
        api_version: str = "2023-10-16",
    ) -> None:
        """
        Initialize Stripe connector.

        Args:
            api_key: Stripe secret API key
            api_version: Stripe API version
        """
        self.api_key = api_key
        self.api_version = api_version
        self._session = None
        self.logger = logger.bind(connector="stripe")

    def connect(self) -> None:
        """Establish connection to Stripe."""
        import httpx

        self._session = httpx.Client(
            base_url=self.BASE_URL,
            auth=(self.api_key, ""),
            headers={
                "Stripe-Version": self.api_version,
            },
            timeout=30.0,
        )
        self.logger.info("Connected to Stripe")

    def disconnect(self) -> None:
        """Close Stripe connection."""
        if self._session:
            self._session.close()
            self._session = None

    def _api_request(
        self,
        method: str,
        endpoint: str,
        params: dict | None = None,
        data: dict | None = None,
    ) -> dict[str, Any]:
        """Make an API request to Stripe."""
        response = self._session.request(
            method=method,
            url=endpoint,
            params=params,
            data=data,
        )
        response.raise_for_status()
        return response.json()

    def _paginate(
        self,
        endpoint: str,
        params: dict | None = None,
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Paginate through Stripe list endpoint."""
        params = params or {}
        params["limit"] = min(100, limit) if limit else 100
        total = 0

        while True:
            response = self._api_request("GET", endpoint, params)

            for item in response.get("data", []):
                yield item
                total += 1
                if limit and total >= limit:
                    return

            if not response.get("has_more"):
                break

            params["starting_after"] = response["data"][-1]["id"]

    def get_customers(
        self,
        limit: int | None = None,
        created_after: datetime | None = None,
        email: str | None = None,
    ) -> pl.DataFrame:
        """
        Extract customer data.

        Args:
            limit: Maximum customers to return
            created_after: Only customers created after this time
            email: Filter by email

        Returns:
            DataFrame with customer data
        """
        params = {}
        if created_after:
            params["created[gte]"] = int(created_after.timestamp())
        if email:
            params["email"] = email

        customers = []
        for customer in self._paginate("/customers", params, limit):
            customers.append({
                "id": customer["id"],
                "email": customer.get("email"),
                "name": customer.get("name"),
                "phone": customer.get("phone"),
                "description": customer.get("description"),
                "created": datetime.fromtimestamp(customer["created"]),
                "currency": customer.get("currency"),
                "default_source": customer.get("default_source"),
                "delinquent": customer.get("delinquent"),
                "balance": customer.get("balance", 0),
                "metadata": customer.get("metadata"),
            })

        return pl.DataFrame(customers) if customers else pl.DataFrame()

    def get_subscriptions(
        self,
        limit: int | None = None,
        status: str | None = None,
        customer: str | None = None,
    ) -> pl.DataFrame:
        """
        Extract subscription data.

        Args:
            limit: Maximum subscriptions
            status: Filter by status (active, canceled, etc.)
            customer: Filter by customer ID

        Returns:
            DataFrame with subscriptions
        """
        params = {}
        if status:
            params["status"] = status
        if customer:
            params["customer"] = customer

        subscriptions = []
        for sub in self._paginate("/subscriptions", params, limit):
            subscriptions.append({
                "id": sub["id"],
                "customer": sub["customer"],
                "status": sub["status"],
                "current_period_start": datetime.fromtimestamp(sub["current_period_start"]),
                "current_period_end": datetime.fromtimestamp(sub["current_period_end"]),
                "cancel_at_period_end": sub.get("cancel_at_period_end"),
                "canceled_at": datetime.fromtimestamp(sub["canceled_at"]) if sub.get("canceled_at") else None,
                "created": datetime.fromtimestamp(sub["created"]),
                "default_payment_method": sub.get("default_payment_method"),
                "latest_invoice": sub.get("latest_invoice"),
                "metadata": sub.get("metadata"),
            })

        return pl.DataFrame(subscriptions) if subscriptions else pl.DataFrame()

    def get_subscription_items(self, subscription_id: str) -> pl.DataFrame:
        """Get items for a subscription."""
        items = []
        for item in self._paginate(f"/subscription_items", {"subscription": subscription_id}):
            items.append({
                "id": item["id"],
                "subscription": item["subscription"],
                "price_id": item["price"]["id"],
                "product_id": item["price"]["product"],
                "quantity": item.get("quantity", 1),
                "created": datetime.fromtimestamp(item["created"]),
            })

        return pl.DataFrame(items) if items else pl.DataFrame()

    def get_charges(
        self,
        limit: int | None = None,
        created_after: datetime | None = None,
        customer: str | None = None,
    ) -> pl.DataFrame:
        """
        Extract charge/payment data.

        Args:
            limit: Maximum charges
            created_after: Only charges after this time
            customer: Filter by customer

        Returns:
            DataFrame with charges
        """
        params = {}
        if created_after:
            params["created[gte]"] = int(created_after.timestamp())
        if customer:
            params["customer"] = customer

        charges = []
        for charge in self._paginate("/charges", params, limit):
            charges.append({
                "id": charge["id"],
                "customer": charge.get("customer"),
                "amount": charge["amount"] / 100,  # Convert from cents
                "currency": charge["currency"],
                "status": charge["status"],
                "paid": charge["paid"],
                "refunded": charge["refunded"],
                "amount_refunded": charge.get("amount_refunded", 0) / 100,
                "created": datetime.fromtimestamp(charge["created"]),
                "description": charge.get("description"),
                "payment_method": charge.get("payment_method"),
                "invoice": charge.get("invoice"),
                "failure_code": charge.get("failure_code"),
                "failure_message": charge.get("failure_message"),
            })

        return pl.DataFrame(charges) if charges else pl.DataFrame()

    def get_invoices(
        self,
        limit: int | None = None,
        status: str | None = None,
        customer: str | None = None,
    ) -> pl.DataFrame:
        """
        Extract invoice data.

        Args:
            limit: Maximum invoices
            status: Filter by status
            customer: Filter by customer

        Returns:
            DataFrame with invoices
        """
        params = {}
        if status:
            params["status"] = status
        if customer:
            params["customer"] = customer

        invoices = []
        for inv in self._paginate("/invoices", params, limit):
            invoices.append({
                "id": inv["id"],
                "customer": inv["customer"],
                "subscription": inv.get("subscription"),
                "status": inv["status"],
                "amount_due": inv["amount_due"] / 100,
                "amount_paid": inv["amount_paid"] / 100,
                "amount_remaining": inv["amount_remaining"] / 100,
                "currency": inv["currency"],
                "created": datetime.fromtimestamp(inv["created"]),
                "due_date": datetime.fromtimestamp(inv["due_date"]) if inv.get("due_date") else None,
                "paid": inv["paid"],
                "period_start": datetime.fromtimestamp(inv["period_start"]),
                "period_end": datetime.fromtimestamp(inv["period_end"]),
                "invoice_pdf": inv.get("invoice_pdf"),
            })

        return pl.DataFrame(invoices) if invoices else pl.DataFrame()

    def get_products(self, limit: int | None = None, active: bool | None = None) -> pl.DataFrame:
        """Extract product catalog."""
        params = {}
        if active is not None:
            params["active"] = str(active).lower()

        products = []
        for product in self._paginate("/products", params, limit):
            products.append({
                "id": product["id"],
                "name": product["name"],
                "description": product.get("description"),
                "active": product["active"],
                "created": datetime.fromtimestamp(product["created"]),
                "default_price": product.get("default_price"),
                "metadata": product.get("metadata"),
            })

        return pl.DataFrame(products) if products else pl.DataFrame()

    def get_prices(self, limit: int | None = None, product: str | None = None) -> pl.DataFrame:
        """Extract price data."""
        params = {}
        if product:
            params["product"] = product

        prices = []
        for price in self._paginate("/prices", params, limit):
            prices.append({
                "id": price["id"],
                "product": price["product"],
                "active": price["active"],
                "currency": price["currency"],
                "unit_amount": price.get("unit_amount", 0) / 100 if price.get("unit_amount") else None,
                "type": price["type"],
                "recurring_interval": price.get("recurring", {}).get("interval"),
                "recurring_interval_count": price.get("recurring", {}).get("interval_count"),
                "created": datetime.fromtimestamp(price["created"]),
            })

        return pl.DataFrame(prices) if prices else pl.DataFrame()

    def get_refunds(
        self,
        limit: int | None = None,
        charge: str | None = None,
    ) -> pl.DataFrame:
        """Extract refund data."""
        params = {}
        if charge:
            params["charge"] = charge

        refunds = []
        for refund in self._paginate("/refunds", params, limit):
            refunds.append({
                "id": refund["id"],
                "charge": refund["charge"],
                "amount": refund["amount"] / 100,
                "currency": refund["currency"],
                "status": refund["status"],
                "reason": refund.get("reason"),
                "created": datetime.fromtimestamp(refund["created"]),
            })

        return pl.DataFrame(refunds) if refunds else pl.DataFrame()

    def get_payment_intents(
        self,
        limit: int | None = None,
        customer: str | None = None,
    ) -> pl.DataFrame:
        """Extract payment intent data."""
        params = {}
        if customer:
            params["customer"] = customer

        intents = []
        for intent in self._paginate("/payment_intents", params, limit):
            intents.append({
                "id": intent["id"],
                "customer": intent.get("customer"),
                "amount": intent["amount"] / 100,
                "currency": intent["currency"],
                "status": intent["status"],
                "created": datetime.fromtimestamp(intent["created"]),
                "payment_method": intent.get("payment_method"),
                "description": intent.get("description"),
            })

        return pl.DataFrame(intents) if intents else pl.DataFrame()

    def get_events(
        self,
        limit: int | None = None,
        event_type: str | None = None,
        created_after: datetime | None = None,
    ) -> pl.DataFrame:
        """
        Extract Stripe events for webhook replay.

        Args:
            limit: Maximum events
            event_type: Filter by type (e.g., 'customer.created')
            created_after: Only events after this time

        Returns:
            DataFrame with events
        """
        params = {}
        if event_type:
            params["type"] = event_type
        if created_after:
            params["created[gte]"] = int(created_after.timestamp())

        events = []
        for event in self._paginate("/events", params, limit):
            events.append({
                "id": event["id"],
                "type": event["type"],
                "created": datetime.fromtimestamp(event["created"]),
                "api_version": event.get("api_version"),
                "object_id": event["data"]["object"].get("id"),
                "object_type": event["data"]["object"].get("object"),
            })

        return pl.DataFrame(events) if events else pl.DataFrame()

    def get_balance_transactions(
        self,
        limit: int | None = None,
        payout: str | None = None,
        created_after: datetime | None = None,
    ) -> pl.DataFrame:
        """Extract balance transaction history."""
        params = {}
        if payout:
            params["payout"] = payout
        if created_after:
            params["created[gte]"] = int(created_after.timestamp())

        transactions = []
        for txn in self._paginate("/balance_transactions", params, limit):
            transactions.append({
                "id": txn["id"],
                "type": txn["type"],
                "amount": txn["amount"] / 100,
                "fee": txn["fee"] / 100,
                "net": txn["net"] / 100,
                "currency": txn["currency"],
                "created": datetime.fromtimestamp(txn["created"]),
                "available_on": datetime.fromtimestamp(txn["available_on"]),
                "source": txn.get("source"),
                "description": txn.get("description"),
            })

        return pl.DataFrame(transactions) if transactions else pl.DataFrame()

    def get_payouts(self, limit: int | None = None) -> pl.DataFrame:
        """Extract payout data."""
        payouts = []
        for payout in self._paginate("/payouts", limit=limit):
            payouts.append({
                "id": payout["id"],
                "amount": payout["amount"] / 100,
                "currency": payout["currency"],
                "status": payout["status"],
                "arrival_date": datetime.fromtimestamp(payout["arrival_date"]),
                "created": datetime.fromtimestamp(payout["created"]),
                "method": payout["method"],
                "type": payout["type"],
            })

        return pl.DataFrame(payouts) if payouts else pl.DataFrame()

    def get_mrr_metrics(self) -> dict[str, Any]:
        """Calculate MRR and subscription metrics."""
        active_subs = self.get_subscriptions(status="active")

        if active_subs.is_empty():
            return {
                "mrr": 0,
                "active_subscriptions": 0,
                "average_revenue_per_subscription": 0,
            }

        # This is a simplified MRR calculation
        # Real implementation would need to fetch subscription items and prices
        return {
            "active_subscriptions": len(active_subs),
            "total_customers": active_subs["customer"].n_unique(),
        }

    def test_connection(self) -> bool:
        """Test the Stripe connection."""
        try:
            self._api_request("GET", "/customers", {"limit": 1})
            return True
        except Exception:
            return False
