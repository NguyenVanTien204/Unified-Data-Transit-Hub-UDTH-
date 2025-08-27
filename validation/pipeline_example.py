"""
Integration example: How to use validation in a real data pipeline.
This shows how to integrate validation into your BigQuery data loading process.
"""

import pandas as pd
import json
from datetime import datetime
import sys
import os

# Add validation module to path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from __init__ import SimpleValidator


def validate_and_load_data(table_name, data_source, output_path=None):
    """
    Complete validation and data preparation pipeline.

    Args:
        table_name: Target BigQuery table name
        data_source: DataFrame or file path with data
        output_path: Where to save cleaned data (optional)

    Returns:
        Dict with validation results and cleaned data info
    """
    print(f"=== Processing {table_name} data ===")

    # Load data if it's a file path
    if isinstance(data_source, str):
        if data_source.endswith('.csv'):
            df = pd.read_csv(data_source)
        elif data_source.endswith('.json'):
            df = pd.read_json(data_source)
        else:
            raise ValueError(f"Unsupported file format: {data_source}")
    else:
        df = data_source.copy()

    print(f"Loaded {len(df)} records")

    # Validate data
    validation_result = SimpleValidator.validate(table_name, df)

    print(f"Validation result: {validation_result['valid']}")
    print(f"Valid rows: {validation_result['valid_rows']}")
    print(f"Invalid rows: {validation_result['invalid_rows']}")

    if validation_result['errors']:
        print("Validation errors:")
        for error in validation_result['errors'][:10]:  # Show first 10 errors
            print(f"  - {error}")

        if len(validation_result['errors']) > 10:
            print(f"  ... and {len(validation_result['errors']) - 10} more errors")

    # Create validation report
    report = {
        'table': table_name,
        'timestamp': datetime.now().isoformat(),
        'total_records': len(df),
        'valid_records': validation_result['valid_rows'],
        'invalid_records': validation_result['invalid_rows'],
        'validation_passed': validation_result['valid'],
        'error_count': len(validation_result['errors']),
        'sample_errors': validation_result['errors'][:5] if validation_result['errors'] else []
    }

    # Save validation report
    report_path = f"validation_report_{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(f"Validation report saved to: {report_path}")

    # If validation passed and output path specified, save cleaned data
    if validation_result['valid'] and output_path:
        df.to_csv(output_path, index=False)
        print(f"Cleaned data saved to: {output_path}")

    return {
        'validation_report': report,
        'data': df,
        'ready_for_bigquery': validation_result['valid']
    }


def create_sample_data():
    """Create sample data for testing."""

    # Sample users data
    users_data = pd.DataFrame([
        {
            'user_id': '123e4567-e89b-12d3-a456-426614174000',
            'name': 'John Doe',
            'email': 'john.doe@example.com',
            'phone': '+1234567890',
            'gender': 'Male',
            'dob': '1990-01-01',
            'country': 'USA',
            'city': 'New York',
            'signup_date': '2023-01-01',
            'is_active': True,
            'customer_tier': 'Gold',
            'total_spent': 5000.50
        },
        {
            'user_id': '223e4567-e89b-12d3-a456-426614174001',
            'name': 'Jane Smith',
            'email': 'jane.smith@example.com',
            'phone': '+1987654321',
            'gender': 'Female',
            'dob': '1985-05-15',
            'country': 'USA',
            'city': 'Los Angeles',
            'signup_date': '2023-02-01',
            'is_active': True,
            'customer_tier': 'Silver',
            'total_spent': 2500.75
        },
        {
            'user_id': 'invalid-uuid-format',  # This will fail validation
            'name': 'Bad User',
            'email': 'invalid-email-format',   # This will fail validation
            'phone': '123',                    # This will fail validation
            'gender': 'Unknown',               # This will fail validation
            'dob': '2030-01-01',              # Future date - will fail
            'country': 'USA',
            'city': 'Chicago',
            'signup_date': '2025-01-01',      # Future date - will fail
            'is_active': 'yes',               # Wrong type - will fail
            'customer_tier': 'Invalid',       # Not in allowed values - will fail
            'total_spent': -100               # Negative value - will fail
        }
    ])

    # Sample orders data
    orders_data = pd.DataFrame([
        {
            'order_id': '123e4567-e89b-12d3-a456-426614174000',
            'user_id': '223e4567-e89b-12d3-a456-426614174001',
            'order_date': '2023-01-15T10:30:00',
            'status': 'delivered',
            'total_amount': 99.99,
            'shipping_fee': 5.99,
            'tax_amount': 8.00,
            'discount_amount': 10.00,
            'payment_status': 'captured',
            'currency': 'USD',
            'shipping_address': '123 Main St, New York, NY 10001',
            'notes': 'Please deliver after 6 PM'
        },
        {
            'order_id': 'invalid-order-id',    # Invalid UUID
            'user_id': 'invalid-user-id',      # Invalid UUID
            'order_date': '2030-01-15T10:30:00',  # Future date
            'status': 'unknown_status',        # Invalid status
            'total_amount': -50.0,            # Negative amount
            'currency': 'INVALID',            # Invalid currency
            'payment_status': 'unknown'       # Invalid payment status
        }
    ])

    return users_data, orders_data


def main():
    """Main pipeline demonstration."""
    print("=== BigQuery Data Validation Pipeline Demo ===\n")

    # Create sample data
    users_df, orders_df = create_sample_data()

    # Process users data
    users_result = validate_and_load_data(
        table_name='users',
        data_source=users_df,
        output_path='cleaned_users.csv'
    )

    print("\n" + "="*50 + "\n")

    # Process orders data
    orders_result = validate_and_load_data(
        table_name='orders',
        data_source=orders_df,
        output_path='cleaned_orders.csv'
    )

    print("\n=== Pipeline Summary ===")
    print(f"Users data ready for BigQuery: {users_result['ready_for_bigquery']}")
    print(f"Orders data ready for BigQuery: {orders_result['ready_for_bigquery']}")

    if users_result['ready_for_bigquery']:
        print("✅ Users data can be loaded to BigQuery")
    else:
        print("❌ Users data needs cleaning before BigQuery load")

    if orders_result['ready_for_bigquery']:
        print("✅ Orders data can be loaded to BigQuery")
    else:
        print("❌ Orders data needs cleaning before BigQuery load")


if __name__ == "__main__":
    main()
