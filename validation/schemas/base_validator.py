"""
Simple validation framework for BigQuery data.
Lightweight schema validation similar to backend systems.
"""

from typing import Dict, List, Any
import pandas as pd
import re
from datetime import datetime


class ValidationError(Exception):
    """Custom exception for validation errors."""
    pass


class SchemaValidator:
    """
    Simple schema validator for data validation before BigQuery ingestion.
    Similar to backend API validation patterns.
    """

    def __init__(self, schema: Dict[str, Any]):
        """
        Initialize validator with schema definition.

        Args:
            schema: Dictionary defining field validation rules
        """
        self.schema = schema
        self.errors = []

    def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a single record against the schema.

        Args:
            data: Dictionary representing a single record

        Returns:
            Dict with validation results
        """
        self.errors = []

        for field_name, rules in self.schema.items():
            value = data.get(field_name)
            field_errors = self._validate_field(field_name, value, rules)
            self.errors.extend(field_errors)

        return {
            'valid': len(self.errors) == 0,
            'errors': self.errors,
            'data': data
        }

    def validate_dataframe(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate entire DataFrame.

        Args:
            df: DataFrame to validate

        Returns:
            Dict with validation results
        """
        all_errors = []
        valid_rows = 0

        for index, row in df.iterrows():
            result = self.validate(row.to_dict())
            if result['valid']:
                valid_rows += 1
            else:
                for error in result['errors']:
                    all_errors.append(f"Row {index}: {error}")

        return {
            'valid': len(all_errors) == 0,
            'errors': all_errors,
            'total_rows': len(df),
            'valid_rows': valid_rows,
            'invalid_rows': len(df) - valid_rows
        }

    def _validate_field(self, field_name: str, value: Any, rules: Dict[str, Any]) -> List[str]:
        """Validate a single field against its rules."""
        errors = []

        # Check required fields
        if rules.get('required', False) and (value is None or pd.isna(value)):
            errors.append(f"{field_name} is required")
            return errors

        # Skip validation if value is None/NaN and field is not required
        if value is None or pd.isna(value):
            return errors

        # Type validation
        field_type = rules.get('type')
        if field_type:
            type_error = self._validate_type(field_name, value, field_type)
            if type_error:
                errors.append(type_error)

        # String validations
        if field_type == 'string':
            errors.extend(self._validate_string(field_name, value, rules))

        # Numeric validations
        elif field_type in ['integer', 'float', 'numeric']:
            errors.extend(self._validate_numeric(field_name, value, rules))

        # Date/datetime validations
        elif field_type in ['date', 'datetime', 'timestamp']:
            errors.extend(self._validate_datetime(field_name, value, rules))

        # Boolean validation
        elif field_type == 'boolean':
            errors.extend(self._validate_boolean(field_name, value, rules))

        # Enum validation
        if 'allowed_values' in rules:
            if value not in rules['allowed_values']:
                errors.append(f"{field_name} must be one of {rules['allowed_values']}")

        # Custom validation function
        if 'validator' in rules:
            try:
                is_valid = rules['validator'](value)
                if not is_valid:
                    errors.append(f"{field_name} failed custom validation")
            except Exception as e:
                errors.append(f"{field_name} validation error: {str(e)}")

        return errors

    def _validate_type(self, field_name: str, value: Any, expected_type: str) -> str:
        """Validate field type."""
        if expected_type == 'string':
            if not isinstance(value, str):
                return f"{field_name} must be a string"

        elif expected_type == 'integer':
            if not isinstance(value, int) or isinstance(value, bool):
                try:
                    int(value)
                except (ValueError, TypeError):
                    return f"{field_name} must be an integer"

        elif expected_type in ['float', 'numeric']:
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                try:
                    float(value)
                except (ValueError, TypeError):
                    return f"{field_name} must be a number"

        elif expected_type == 'boolean':
            if not isinstance(value, bool):
                if str(value).lower() not in ['true', 'false', '1', '0']:
                    return f"{field_name} must be a boolean"

        elif expected_type in ['date', 'datetime', 'timestamp']:
            try:
                pd.to_datetime(value)
            except (ValueError, TypeError):
                return f"{field_name} must be a valid date/datetime"

        return None

    def _validate_string(self, field_name: str, value: Any, rules: Dict[str, Any]) -> List[str]:
        """Validate string field."""
        errors = []
        str_value = str(value)

        # Length validation
        if 'min_length' in rules and len(str_value) < rules['min_length']:
            errors.append(f"{field_name} must be at least {rules['min_length']} characters")

        if 'max_length' in rules and len(str_value) > rules['max_length']:
            errors.append(f"{field_name} must be at most {rules['max_length']} characters")

        # Pattern validation
        if 'pattern' in rules:
            if not re.match(rules['pattern'], str_value):
                errors.append(f"{field_name} does not match required pattern")

        # Format validation
        format_type = rules.get('format')
        if format_type == 'email':
            if not self._validate_email(str_value):
                errors.append(f"{field_name} must be a valid email address")

        elif format_type == 'uuid':
            if not self._validate_uuid(str_value):
                errors.append(f"{field_name} must be a valid UUID")

        elif format_type == 'phone':
            if not self._validate_phone(str_value):
                errors.append(f"{field_name} must be a valid phone number")

        return errors

    def _validate_numeric(self, field_name: str, value: Any, rules: Dict[str, Any]) -> List[str]:
        """Validate numeric field."""
        errors = []

        try:
            num_value = float(value)
        except (ValueError, TypeError):
            return [f"{field_name} must be a number"]

        # Range validation
        if 'min_value' in rules and num_value < rules['min_value']:
            errors.append(f"{field_name} must be at least {rules['min_value']}")

        if 'max_value' in rules and num_value > rules['max_value']:
            errors.append(f"{field_name} must be at most {rules['max_value']}")

        return errors

    def _validate_datetime(self, field_name: str, value: Any, rules: Dict[str, Any]) -> List[str]:
        """Validate datetime field."""
        errors = []

        try:
            dt_value = pd.to_datetime(value)
        except (ValueError, TypeError):
            return [f"{field_name} must be a valid date/datetime"]

        # Date range validation
        if 'min_date' in rules:
            min_date = pd.to_datetime(rules['min_date'])
            if dt_value < min_date:
                errors.append(f"{field_name} must be after {rules['min_date']}")

        if 'max_date' in rules:
            max_date = pd.to_datetime(rules['max_date'])
            if dt_value > max_date:
                errors.append(f"{field_name} must be before {rules['max_date']}")

        return errors

    def _validate_boolean(self, field_name: str, value: Any, rules: Dict[str, Any]) -> List[str]:
        """Validate boolean field."""
        errors = []

        if not isinstance(value, bool):
            str_value = str(value).lower()
            if str_value not in ['true', 'false', '1', '0']:
                errors.append(f"{field_name} must be a boolean value")

        return errors

    def _validate_email(self, email: str) -> bool:
        """Validate email format."""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return re.match(pattern, email) is not None

    def _validate_uuid(self, uuid_str: str) -> bool:
        """Validate UUID format."""
        pattern = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        return re.match(pattern, uuid_str) is not None

    def _validate_phone(self, phone: str) -> bool:
        """Validate phone number format."""
        # Remove all non-digit characters
        digits_only = re.sub(r'\D', '', phone)
        # Check if it's between 10-15 digits
        return 10 <= len(digits_only) <= 15
