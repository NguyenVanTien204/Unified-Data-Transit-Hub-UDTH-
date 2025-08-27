"""
Main validation module for BigQuery data validation.
Simple import and usage.
"""

# Re-export main components for easy access
from schemas.base_validator import SchemaValidator
from schemas.users_schema import USERS_SCHEMA
from schemas.orders_schema import ORDERS_SCHEMA
from schemas.events_schema import EVENTS_SCHEMA
from schemas.messages_schema import MESSAGES_SCHEMA
from schemas.products_schema import PRODUCTS_SCHEMA


class SimpleValidator:
    """
    Simplified validator interface similar to backend validation frameworks.
    """

    SCHEMAS = {
        'users': USERS_SCHEMA,
        'orders': ORDERS_SCHEMA,
        'events': EVENTS_SCHEMA,
        'messages': MESSAGES_SCHEMA,
        'products': PRODUCTS_SCHEMA
    }

    @classmethod
    def validate(cls, table_name, data):
        """
        Quick validation method.

        Args:
            table_name: Name of the table schema to use
            data: Single record (dict) or DataFrame to validate

        Returns:
            Validation result dict
        """
        if table_name not in cls.SCHEMAS:
            return {
                'valid': False,
                'error': f"Unknown table: {table_name}. Available: {list(cls.SCHEMAS.keys())}"
            }

        validator = SchemaValidator(cls.SCHEMAS[table_name])

        # Check if it's a DataFrame or dict
        if hasattr(data, 'iterrows'):  # DataFrame
            return validator.validate_dataframe(data)
        else:  # Single record
            return validator.validate(data)

    @classmethod
    def get_schema(cls, table_name):
        """Get schema definition for a table."""
        return cls.SCHEMAS.get(table_name, {})

    @classmethod
    def list_tables(cls):
        """List available table schemas."""
        return list(cls.SCHEMAS.keys())


# Convenience functions for each table type
def validate_users(data):
    """Validate users data."""
    return SimpleValidator.validate('users', data)


def validate_orders(data):
    """Validate orders data."""
    return SimpleValidator.validate('orders', data)


def validate_events(data):
    """Validate events data."""
    return SimpleValidator.validate('events', data)


def validate_messages(data):
    """Validate messages data."""
    return SimpleValidator.validate('messages', data)


def validate_products(data):
    """Validate products data."""
    return SimpleValidator.validate('products', data)


# Export main components
__all__ = [
    'SimpleValidator',
    'SchemaValidator',
    'validate_users',
    'validate_orders',
    'validate_events',
    'validate_messages',
    'validate_products',
    'USERS_SCHEMA',
    'ORDERS_SCHEMA',
    'EVENTS_SCHEMA',
    'MESSAGES_SCHEMA',
    'PRODUCTS_SCHEMA'
]
