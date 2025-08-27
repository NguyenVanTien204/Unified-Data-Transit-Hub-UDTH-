"""
Simple orders table validation schema.
"""

# Orders table schema definition
ORDERS_SCHEMA = {
    'order_id': {
        'type': 'string',
        'required': True,
        'format': 'uuid'
    },
    'user_id': {
        'type': 'string',
        'required': True,
        'format': 'uuid'
    },
    'order_date': {
        'type': 'timestamp',
        'required': True,
        'min_date': '2020-01-01'
    },
    'status': {
        'type': 'string',
        'required': True,
        'allowed_values': ['pending', 'confirmed', 'processing', 'shipped', 'delivered', 'cancelled', 'returned']
    },
    'total_amount': {
        'type': 'numeric',
        'required': True,
        'min_value': 0,
        'max_value': 100000
    },
    'shipping_fee': {
        'type': 'numeric',
        'required': False,
        'min_value': 0,
        'max_value': 1000
    },
    'tax_amount': {
        'type': 'numeric',
        'required': False,
        'min_value': 0,
        'max_value': 10000
    },
    'discount_amount': {
        'type': 'numeric',
        'required': False,
        'min_value': 0,
        'max_value': 50000
    },
    'payment_status': {
        'type': 'string',
        'required': False,
        'allowed_values': ['pending', 'authorized', 'captured', 'failed', 'refunded', 'partially_refunded']
    },
    'currency': {
        'type': 'string',
        'required': True,
        'allowed_values': ['USD', 'EUR', 'GBP', 'JPY', 'VND', 'AUD', 'CAD']
    },
    'shipping_address': {
        'type': 'string',
        'required': False,
        'max_length': 500
    },
    'notes': {
        'type': 'string',
        'required': False,
        'max_length': 1000
    }
}
