"""
Simple products table validation schema.
"""

# Products table schema definition
PRODUCTS_SCHEMA = {
    'product_id': {
        'type': 'string',
        'required': True,
        'format': 'uuid'
    },
    'name': {
        'type': 'string',
        'required': True,
        'max_length': 200
    },
    'category': {
        'type': 'string',
        'required': True,
        'max_length': 100
    },
    'brand': {
        'type': 'string',
        'required': False,
        'max_length': 100
    },
    'price': {
        'type': 'numeric',
        'required': True,
        'min_value': 0,
        'max_value': 100000
    },
    'description': {
        'type': 'string',
        'required': False,
        'max_length': 5000
    },
    'available': {
        'type': 'boolean',
        'required': True
    },
    'created_at': {
        'type': 'timestamp',
        'required': True,
        'min_date': '2020-01-01'
    },
    'updated_at': {
        'type': 'timestamp',
        'required': False,
        'min_date': '2020-01-01'
    },
    'is_active': {
        'type': 'boolean',
        'required': True
    },
    'status': {
        'type': 'string',
        'required': False,
        'allowed_values': ['active', 'inactive', 'discontinued', 'out_of_stock']
    },
    'featured': {
        'type': 'boolean',
        'required': False
    },
    'sales_count': {
        'type': 'integer',
        'required': False,
        'min_value': 0
    },
    'view_count': {
        'type': 'integer',
        'required': False,
        'min_value': 0
    }
}
