# BigQuery Data Validation Framework

Một hệ thống validation đơn giản cho dữ liệu trước khi tải vào BigQuery, thiết kế theo pattern của các hệ thống backend thông thường.

## Cấu trúc thư mục

```
validation/
├── schemas/
│   ├── __init__.py              # Exports chính
│   ├── base_validator.py        # Class validator cơ bản
│   ├── users_schema.py          # Schema cho bảng users
│   ├── orders_schema.py         # Schema cho bảng orders
│   ├── events_schema.py         # Schema cho bảng events
│   ├── messages_schema.py       # Schema cho bảng messages
│   └── products_schema.py       # Schema cho bảng products
├── validator.py                 # Validator chính và convenience functions
├── example_usage.py            # Ví dụ sử dụng
└── README.md                   # Tài liệu này
```

## Cách sử dụng

### 1. Validation đơn giản cho một record

```python
from validation.validator import validate_user_record

# Dữ liệu user hợp lệ
user_data = {
    'user_id': '123e4567-e89b-12d3-a456-426614174000',
    'name': 'John Doe',
    'email': 'john.doe@example.com',
    'signup_date': '2023-01-01',
    'is_active': True,
    'total_spent': 1500.0
}

result = validate_user_record(user_data)
print(result)
# Output: {'valid': True, 'errors': [], 'data': {...}}
```

### 2. Validation cho DataFrame

```python
import pandas as pd
from validation.validator import validate_users_data

# Tạo DataFrame mẫu
df = pd.DataFrame([
    {
        'user_id': '123e4567-e89b-12d3-a456-426614174000',
        'email': 'john@example.com',
        'signup_date': '2023-01-01',
        'is_active': True
    },
    {
        'user_id': 'invalid-uuid',  # Sẽ fail validation
        'email': 'not-an-email',   # Sẽ fail validation
        'signup_date': '2023-01-02',
        'is_active': True
    }
])

result = validate_users_data(df)
print(result)
# Output: {'valid': False, 'errors': [...], 'total_rows': 2, 'valid_rows': 1, 'invalid_rows': 1}
```

### 3. Sử dụng Main Validator

```python
from validation.validator import BigQueryValidator

validator = BigQueryValidator()

# Liệt kê các bảng có sẵn
print(validator.list_tables())
# Output: ['users', 'orders', 'events', 'messages', 'products']

# Lấy schema của một bảng
schema = validator.get_schema('users')
print(schema.keys())

# Validate dữ liệu cho bảng cụ thể
result = validator.validate_table('orders', orders_df)
```

## Schema Definitions

### Users Schema
- `user_id`: string, required, UUID format
- `name`: string, optional, max 100 chars
- `email`: string, required, email format
- `phone`: string, optional, phone format
- `gender`: string, optional, enum values
- `dob`: date, optional, date range validation
- `signup_date`: date, required, business date range
- `is_active`: boolean, required
- `customer_tier`: string, optional, enum values
- `total_spent`: numeric, optional, range validation

### Orders Schema
- `order_id`: string, required, UUID format
- `user_id`: string, required, UUID format
- `order_date`: timestamp, required, date range
- `status`: string, required, enum values
- `total_amount`: numeric, required, positive values
- `currency`: string, required, enum values
- Các trường khác...

### Events Schema
- `event_id`: string, required, UUID format
- `user_id`: string, required, UUID format
- `event_type`: string, required, enum values
- `timestamp`: timestamp, required
- Các trường khác...

## Tính năng chính

### 1. Validation Types
- **Type validation**: Kiểm tra kiểu dữ liệu
- **Required fields**: Kiểm tra trường bắt buộc
- **Format validation**: Email, UUID, phone number
- **Range validation**: Min/max values cho số và ngày
- **Enum validation**: Giá trị cho phép
- **Custom validation**: Functions tùy chỉnh

### 2. Validation Rules
```python
field_rules = {
    'type': 'string',           # Kiểu dữ liệu
    'required': True,           # Bắt buộc
    'max_length': 100,          # Độ dài tối đa
    'min_length': 5,            # Độ dài tối thiểu
    'format': 'email',          # Format đặc biệt
    'allowed_values': [...],    # Giá trị cho phép
    'min_value': 0,             # Giá trị tối thiểu (số)
    'max_value': 1000,          # Giá trị tối đa (số)
    'min_date': '2020-01-01',   # Ngày tối thiểu
    'max_date': '2025-12-31',   # Ngày tối đa
    'pattern': r'^[A-Z]+$',     # Regex pattern
    'validator': lambda x: ..., # Custom function
}
```

### 3. Response Format
```python
{
    'valid': True/False,        # Kết quả validation
    'errors': [...],            # Danh sách lỗi
    'total_rows': 100,          # Tổng số dòng (DataFrame)
    'valid_rows': 95,           # Số dòng hợp lệ
    'invalid_rows': 5,          # Số dòng không hợp lệ
    'data': {...}               # Dữ liệu gốc (single record)
}
```

## Mở rộng Schema

Để thêm bảng mới:

1. Tạo file schema mới trong `schemas/`:
```python
# schemas/new_table_schema.py
NEW_TABLE_SCHEMA = {
    'field1': {
        'type': 'string',
        'required': True
    },
    # ... other fields
}
```

2. Thêm vào `__init__.py`:
```python
from .new_table_schema import NEW_TABLE_SCHEMA
__all__.append('NEW_TABLE_SCHEMA')
```

3. Thêm vào `validator.py`:
```python
def __init__(self):
    self.validators = {
        # ... existing validators
        'new_table': SchemaValidator(NEW_TABLE_SCHEMA)
    }
```

## Chạy ví dụ

```bash
cd validation
python example_usage.py
```

## Lợi ích

1. **Đơn giản**: API dễ sử dụng như backend validation
2. **Linh hoạt**: Dễ mở rộng và tùy chỉnh
3. **Reliable**: Comprehensive validation rules
4. **Fast**: Lightweight và hiệu suất cao
5. **Maintainable**: Code sạch, có cấu trúc rõ ràng

Framework này cung cấp nền tảng vững chắc để đảm bảo chất lượng dữ liệu trước khi tải vào BigQuery, giúp tránh lỗi và đảm bảo tính nhất quán của dữ liệu.
