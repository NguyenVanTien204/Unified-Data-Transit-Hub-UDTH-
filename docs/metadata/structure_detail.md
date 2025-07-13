# Project Folder Structure


```
elt_project/
├── extractors/
│   ├── airbyte_config/
│   │   ├── mysql_source.json
│   │   ├── mongodb_source.json
│   │   └── destinations/
│   │       └── warehouse_postgres.json
│   └── scripts/
│       ├── test_mysql_connection.py
│       └── manual_extractor.py
│
├── transform/
│   ├── dbt_project/
│   │   ├── dbt_project.yml
│   │   ├── models/
│   │   │   ├── staging/
│   │   │   │   └── stg_users.sql
│   │   │   ├── intermediate/
│   │   │   │   └── int_user_orders.sql
│   │   │   ├── marts/
│   │   │   │   ├── dim_users.sql
│   │   │   │   ├── fact_orders.sql
│   │   │   │   └── fact_user_interactions.sql
│   │   └── seeds/
│   │       └── country_codes.csv
│
├── validation/
│   ├── dbt_tests/
│   │   ├── tests/
│   │   │   ├── not_null_user_id.sql
│   │   │   ├── unique_email.sql
│   │   │   └── accepted_values_order_status.sql
│   │   └── schema.yml
│   └── great_expectations/         # (optional, if using GE)
│       ├── expectations/
│       └── checkpoints/
│
├── orchestration/
│   ├── dags/
│   │   └── elt_pipeline_dag.py
│   ├── configs/
│   │   ├── variables.json
│   │   └── connections.json
│   └── utils/
│       └── slack_alert.py
│
├── docs/
│   ├── metadata/
│   │   ├── table_descriptions.md
│   │   ├── lineage_diagram.drawio
│   │   └── glossary.md
│   └── catalog/
│       ├── dbt_catalog.json
│       └── dbt_lineage.json
│
├── bi_dashboards/
│   ├── metabase/
│   │   ├── orders_dashboard.json
│   │   └── user_activity_dashboard.json
│   └── superset/
│       ├── sales_insights_dashboard.yaml
│       └── campaign_performance.yaml
│
└── README.md
```

## Detailed explanation of each module
### `extractors/` – **Dữ liệu gốc**

- Airbyte: cấu hình `.json` cho các nguồn (MySQL, MongoDB, API, ...).
    
- Script ETL thủ công: backup nếu không dùng Airbyte.
    
- Có thể thêm test kết nối, kiểm tra schema mapping.
    

### `transform/` – **Xử lý dữ liệu (dbt)**

- `staging/`: raw → cleaned table (đổi tên, parse JSON, typecast)
    
- `intermediate/`: logic xử lý trung gian (join, calculate)
    
- `marts/`: bảng cuối `fact_*`, `dim_*`
    
- `seeds/`: file tĩnh `.csv` dùng làm lookup table
    

### `validation/` – **Chất lượng dữ liệu**

- `dbt tests`: check ràng buộc khóa, giá trị hợp lệ, null, unique
    
- `great_expectations`: nếu bạn dùng để test dữ liệu trước khi load vào warehouse
    
- Có thể thêm `data contract` YAML/schema nếu cần
    

### `orchestration/` – **Tự động hóa (Airflow)**

- `dags/`: DAG thực thi toàn bộ pipeline
    
- `configs/`: lưu biến môi trường hoặc kết nối
    
- `utils/`: hàm phụ trợ như alert, retry, báo lỗi
    

### `docs/` – **Tài liệu**

- Metadata: mô tả bảng, lineage, business logic
    
- Catalog: export từ dbt, dùng với DataHub, OpenMetadata nếu cần
    

### `bi_dashboards/` – **Trực quan hóa**

- `metabase/`: dashboard JSON export
    
- `superset/`: YAML export từ dashboard
    
- Có thể thêm ảnh chụp minh họa