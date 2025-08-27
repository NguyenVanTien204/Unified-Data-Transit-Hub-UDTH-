# Hướng dẫn sử dụng Validation Framework

## Tổng quan

Đã thiết kế thành công một bộ khung validation schema đơn giản cho BigQuery với:

- ✅ **API đơn giản** giống như backend validation
- ✅ **Schema definitions** cho 5 bảng chính
- ✅ **Validation rules** comprehensive
- ✅ **Easy integration** vào data pipeline
- ✅ **Error reporting** chi tiết

## Cấu trúc hoàn chỉnh

```
validation/
├── schemas/
│   ├── __init__.py              # Schema exports
│   ├── base_validator.py        # Core validation engine
│   ├── users_schema.py          # Users table schema
│   ├── orders_schema.py         # Orders table schema
│   ├── events_schema.py         # Events table schema
│   ├── messages_schema.py       # Messages table schema
│   └── products_schema.py       # Products table schema
├── __init__.py                  # Main module exports
├── validator.py                 # Original complex validator
├── demo.py                      # Simple usage demo
├── test_simple.py              # Basic testing
├── pipeline_example.py         # Real pipeline integration
├── example_usage.py            # Original examples
└── README.md                   # Documentation
```

## Cách sử dụng cơ bản

### 1. Import và sử dụng đơn giản

```python
import sys
sys.path.append('path/to/validation')

from __init__ import SimpleValidator, validate_users

# Validate single record
user = {
    'user_id': '123e4567-e89b-12d3-a456-426614174000',
    'email': 'user@example.com',
    'signup_date': '2023-01-01',
    'is_active': True
}

result = validate_users(user)
print(f"Valid: {result['valid']}")
```

### 2. Validate DataFrame

```python
import pandas as pd

df = pd.read_csv('users_data.csv')
result = SimpleValidator.validate('users', df)

print(f"Total: {result['total_rows']}")
print(f"Valid: {result['valid_rows']}")
print(f"Invalid: {result['invalid_rows']}")
```

### 3. Pipeline Integration

```python
def process_data_for_bigquery(table_name, data_path):
    # Load data
    df = pd.read_csv(data_path)

    # Validate
    result = SimpleValidator.validate(table_name, df)

    if result['valid']:
        print("✅ Data ready for BigQuery")
        return df
    else:
        print("❌ Data has validation errors:")
        for error in result['errors'][:10]:
            print(f"  - {error}")
        return None
```

## Schema Validation Rules

### Users Table
- `user_id`: UUID format, required
- `email`: Email format, required, unique
- `phone`: Phone format, optional
- `gender`: Enum values, optional
- `dob`: Date range 1900-2010, optional
- `signup_date`: Date >= 2020-01-01, required
- `is_active`: Boolean, required
- `customer_tier`: Enum values, optional
- `total_spent`: >= 0, optional

### Orders Table
- `order_id`: UUID format, required
- `user_id`: UUID format, required
- `order_date`: Timestamp >= 2020-01-01, required
- `status`: Enum values, required
- `total_amount`: >= 0, required
- `currency`: Enum values, required
- `payment_status`: Enum values, optional

### Events Table
- `event_id`: UUID format, required
- `user_id`: UUID format, required
- `event_type`: Enum values, required
- `timestamp`: Timestamp >= 2020-01-01, required
- `duration_seconds`: 0-86400, optional
- `device_type`: Enum values, optional

## Các validation rules được hỗ trợ

```python
field_rules = {
    'type': 'string|integer|float|numeric|boolean|date|datetime|timestamp',
    'required': True|False,
    'format': 'email|uuid|phone',
    'max_length': 100,
    'min_length': 5,
    'allowed_values': ['value1', 'value2'],
    'min_value': 0,
    'max_value': 1000,
    'min_date': '2020-01-01',
    'max_date': '2025-12-31',
    'pattern': r'^[A-Z]+$',
    'validator': lambda x: custom_validation(x)
}
```

## Kết quả validation

```python
{
    'valid': True|False,           # Tổng kết quả
    'errors': [...],               # Danh sách lỗi chi tiết
    'total_rows': 100,             # Tổng số dòng (DataFrame)
    'valid_rows': 95,              # Số dòng hợp lệ
    'invalid_rows': 5,             # Số dòng có lỗi
    'data': {...}                  # Dữ liệu gốc (single record)
}
```

## Demo files đã tạo

1. **`demo.py`** - Cách sử dụng cơ bản
2. **`test_simple.py`** - Testing validation rules
3. **`pipeline_example.py`** - Integration với data pipeline thực tế

## Chạy thử

```bash
cd validation/
python demo.py                    # Demo cơ bản
python test_simple.py             # Test validation
python pipeline_example.py       # Pipeline integration demo
```

## Lợi ích

1. **Đơn giản**: API giống backend frameworks (Django, FastAPI)
2. **Linh hoạt**: Dễ thêm bảng mới và rules mới
3. **Comprehensive**: Cover đầy đủ validation cases
4. **Production-ready**: Có error handling và reporting
5. **Maintainable**: Code clean, well-structured

Framework này cung cấp nền tảng vững chắc để validate dữ liệu trước khi load vào BigQuery, đảm bảo data quality và tránh lỗi trong quá trình ETL.
