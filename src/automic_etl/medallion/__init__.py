"""Medallion architecture implementation for Automic ETL."""

from automic_etl.medallion.bronze import BronzeLayer
from automic_etl.medallion.silver import SilverLayer
from automic_etl.medallion.gold import GoldLayer
from automic_etl.medallion.lakehouse import Lakehouse
from automic_etl.medallion.scd import SCDType2Manager

__all__ = [
    "BronzeLayer",
    "SilverLayer",
    "GoldLayer",
    "Lakehouse",
    "SCDType2Manager",
]
