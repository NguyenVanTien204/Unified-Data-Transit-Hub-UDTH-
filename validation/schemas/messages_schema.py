"""
Simple messages table validation schema.
"""

# Messages table schema definition
MESSAGES_SCHEMA = {
    'thread_id': {
        'type': 'string',
        'required': True,
        'format': 'uuid'
    },
    'user_id': {
        'type': 'string',
        'required': True,
        'format': 'uuid'
    },
    'conversation_type': {
        'type': 'string',
        'required': True,
        'allowed_values': ['support', 'sales', 'general', 'complaint', 'feedback']
    },
    'status': {
        'type': 'string',
        'required': True,
        'allowed_values': ['open', 'in_progress', 'resolved', 'closed', 'escalated']
    },
    'priority': {
        'type': 'string',
        'required': False,
        'allowed_values': ['low', 'medium', 'high', 'urgent']
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
    'satisfaction_rating': {
        'type': 'integer',
        'required': False,
        'min_value': 1,
        'max_value': 5
    },
    'resolved': {
        'type': 'boolean',
        'required': False
    },
    'resolution_time_minutes': {
        'type': 'integer',
        'required': False,
        'min_value': 0,
        'max_value': 43200  # Max 30 days
    }
}
