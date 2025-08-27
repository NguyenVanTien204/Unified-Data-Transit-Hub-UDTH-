"""
Simple validation schemas for BigQuery data ingestion.
This package contains lightweight validation schemas for ensuring data quality
before loading into BigQuery analytics tables.
"""

from .base_validator import SchemaValidator
from .users_schema import USERS_SCHEMA
from .orders_schema import ORDERS_SCHEMA
from .events_schema import EVENTS_SCHEMA
from .messages_schema import MESSAGES_SCHEMA
from .products_schema import PRODUCTS_SCHEMA

__all__ = [
    'SchemaValidator',
    'USERS_SCHEMA',
    'ORDERS_SCHEMA',
    'EVENTS_SCHEMA',
    'MESSAGES_SCHEMA',
    'PRODUCTS_SCHEMA'
]
