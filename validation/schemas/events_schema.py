"""
Simple events table validation schema.
"""

# Events table schema definition
EVENTS_SCHEMA = {
    'event_id': {
        'type': 'string',
        'required': True,
        'format': 'uuid'
    },
    'user_id': {
        'type': 'string',
        'required': True,
        'format': 'uuid'
    },
    'session_id': {
        'type': 'string',
        'required': False,
        'format': 'uuid'
    },
    'event_type': {
        'type': 'string',
        'required': True,
        'allowed_values': ['page_view', 'click', 'purchase', 'login', 'logout', 'search', 'add_to_cart', 'checkout']
    },
    'timestamp': {
        'type': 'timestamp',
        'required': True,
        'min_date': '2020-01-01'
    },
    'duration_seconds': {
        'type': 'integer',
        'required': False,
        'min_value': 0,
        'max_value': 86400  # Max 24 hours
    },
    'success': {
        'type': 'boolean',
        'required': False
    },
    'device_type': {
        'type': 'string',
        'required': False,
        'allowed_values': ['desktop', 'mobile', 'tablet', 'smart_tv', 'smartwatch']
    },
    'page_url': {
        'type': 'string',
        'required': False,
        'max_length': 2000
    }
}
