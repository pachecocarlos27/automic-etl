"""API connectors for Automic ETL."""

from automic_etl.connectors.api.rest import RESTConnector, WebhookReceiver
from automic_etl.connectors.api.salesforce import SalesforceConnector
from automic_etl.connectors.api.hubspot import HubSpotConnector
from automic_etl.connectors.api.stripe import StripeConnector

__all__ = [
    "RESTConnector",
    "WebhookReceiver",
    "SalesforceConnector",
    "HubSpotConnector",
    "StripeConnector",
]
