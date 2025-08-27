"""
Simple users table validation schema.
"""

# Users table schema definition
USERS_SCHEMA = {
    'user_id': {
        'type': 'string',
        'required': True,
        'format': 'uuid'
    },
    'name': {
        'type': 'string',
        'required': False,
        'max_length': 100
    },
    'email': {
        'type': 'string',
        'required': True,
        'format': 'email',
        'max_length': 150
    },
    'phone': {
        'type': 'string',
        'required': False,
        'format': 'phone'
    },
    'gender': {
        'type': 'string',
        'required': False,
        'allowed_values': ['Male', 'Female', 'Other', 'Prefer not to say']
    },
    'dob': {
        'type': 'date',
        'required': False,
        'min_date': '1900-01-01',
        'max_date': '2010-12-31'
    },
    'country': {
        'type': 'string',
        'required': False,
        'max_length': 100
    },
    'city': {
        'type': 'string',
        'required': False,
        'max_length': 100
    },
    'signup_date': {
        'type': 'date',
        'required': True,
        'min_date': '2020-01-01'
    },
    'is_active': {
        'type': 'boolean',
        'required': True
    },
    'customer_tier': {
        'type': 'string',
        'required': False,
        'allowed_values': ['Bronze', 'Silver', 'Gold', 'Platinum', 'Diamond']
    },
    'total_spent': {
        'type': 'numeric',
        'required': False,
        'min_value': 0,
        'max_value': 1000000
    }
}
