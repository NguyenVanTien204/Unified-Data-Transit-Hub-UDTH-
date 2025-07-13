_Hệ thống trung chuyển và chuẩn hóa dữ liệu phức tạp từ nhiều cơ sở dữ liệu về kho dữ liệu phân tích tập trung_


### Ý tưởng cốt lõi
>Nhiều hệ thống trong doanh nghiệp (CRM, E-commerce, logistics, marketing...) lưu dữ liệu phân tán ở các cơ sở dữ liệu khác nhau như MySQL, MongoDB, Oracle, Firebase,... Mục tiêu là:

- Tự động trích xuất
- Làm sạch và chuẩn hóa schema dị biệt
- Tích hợp thành unified fact tables và dimension tables
- Lưu vào **Data Warehouse (PostgreSQL, BigQuery, Snowflake, etc.)** để dùng cho BI, ML hoặc kiểm toán.

### Kịch bản
Các hệ thống giả định và loại dữ liệu:

| Hệ thống           | Nguồn dữ liệu | Dữ liệu quan trọng         |
| ------------------ | ------------- | -------------------------- |
| CRM                | PostgreSQL    | user profile, interactions |
| E-commerce         | MySQL         | orders, transactions       |
| Inventory          | MongoDB       | products, stock levels     |
| Feedback & support | Firebase      | user messages, ratings     |
| 3rd-party          | REST API      | marketing campaign data    |
### Dữ liệu

>Có thể gen dữ liệu bằng các thư viện python sau rồi import chúng vào các database khác nhau

| Thư viện                                   | Mô tả chính                                                                    | Hỗ trợ        |
| ------------------------------------------ | ------------------------------------------------------------------------------ | ------------- |
| `Faker`                                    | Sinh dữ liệu ngẫu nhiên kiểu đơn giản (tên, địa chỉ, email...)                 | Structured    |
| `Mimesis`                                  | Tương tự `Faker`, nhưng đa dạng dữ liệu và hỗ trợ multi-locale tốt hơn         | Structured    |
| `Faker-Factory-Boy`                        | Cho phép tạo quan hệ giữa các thực thể (user → order)                          | ORM-style     |
| `JSONSchema Faker` (`py-jsonschema-faker`) | Tạo dữ liệu theo mẫu JSON schema định nghĩa sẵn                                | JSON phức tạp |
| `hypothesis`                               | Sinh dữ liệu để kiểm thử, rất mạnh trong việc sinh dữ liệu có ràng buộc        | Logic         |
| `Datafaker`                                | Dùng cho Java/Spark, nhưng có thể mô phỏng cho Big Data (nếu dùng PySpark)     | Java          |
| `pydantic-factories`                       | Tạo dữ liệu từ model Pydantic, hữu ích khi bạn dùng FastAPI hoặc data contract | Typed, JSON   |
| `factory_boy` + Faker                      | Dùng cho quan hệ mô phỏng ORM → orders từ user                                 | Relationship  |
| `random-objects`                           | Gen ra object dạng dict lồng nhau với config đơn giản                          |  Nested JSON  |
### Kiến trúc dự kiến
```
    +--------------+     +-------------+     +------------+
    |   PostgreSQL |     |    MySQL    |     |  MongoDB   |
    +------+-------+     +------+------+     +------+-----+
           |                    |                   |
           |                    |                   |
           +--------+-----------+-----------+-------+
                    |                       |
                [Airbyte/Fivetran]     [Custom Python Extractor]
                    |                       |
                    +----------+------------+
                               |
                        [Staging Layer]
                      (Raw tables or Lake)
                               |
                         [Transform Layer]
            (dbt, Spark, Pandas, or SQL in DWH engine)
                               |
                         [Data Warehouse]
                    (Star Schema: fact + dims)
                               |
                        [BI tools, ML system]

```

### Chi tiết pipeline
| Giai đoạn     | Mô tả                                                                 | Công cụ đề xuất                                                           |
| ------------- | --------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| **Extract**   | Kết nối và lấy dữ liệu từ nhiều nguồn (SQL, NoSQL, API)               | **Airbyte** cho MySQL/Postgres/API, custom extractor cho MongoDB/Firebase |
| **Load**      | Đưa vào staging layer trong DWH (raw tables)                          | PostgreSQL DWH, BigQuery hoặc Snowflake                                   |
| **Transform** | Chuẩn hóa schema, xử lý logic nghiệp vụ, sinh các `fact_*` và `dim_*` | **dbt** (Data Build Tool), hoặc Spark nếu khối lượng lớn                  |
| **Validate**  | So sánh số lượng, schema matching, kiểm tra lỗi                       | Custom script hoặc dbt tests                                              |
| **Serve**     | Truy vấn, dashboard hoặc gửi sang downstream                          | Metabase, Superset, Looker                                                |

### Các yếu tố phức tạp
- **Schema không đồng nhất**:
    
    - MongoDB có dữ liệu lồng nhau → yêu cầu flatten + mapping sang bảng quan hệ.
        
    - Firebase có timestamp/format khác → xử lý chuẩn hoá kiểu dữ liệu.
        
- **Conflict Resolution**:
    
    - User ID bị trùng giữa hệ thống → phải có logic mapping/namespace hóa.
        
    - Dùng `unified_id` cho thực thể chung (người dùng, sản phẩm,...)
        
- **Tự động incremental load**:
    
    - MySQL/Postgres: dùng `last_updated_at`
        
    - MongoDB: dùng `_id.generation_time` hoặc change tracking
        
    - Firebase/API: dùng timestamp hoặc paging
        
- **Version hóa dữ liệu (Slowly Changing Dimensions - SCD)**:
    
    - Ví dụ: user đổi email, hệ thống cần lưu lại lịch sử
        
- **Metadata & lineage tracking**:
    
    - Mỗi bản ghi cần log: `source`, `extract_time`, `load_batch_id`, etc.

### Cấu trúc schema warehouse
> Thiết kế warehouse áp dụng mô hình star schema với các bảng dim và fact
- `fact_orders`: Từ MySQL
- `dim_users`: Từ PostgreSQL CRM
- `dim_products`: Từ MongoDB
- `dim_channels`: Từ API
- `fact_feedback`: Từ Firebase

 ```
              +--------------+
              |  dim_users   |
              +--------------+
                     |
                     v
              +--------------+
              | fact_orders  |
              +--------------+
                     ^
                     |
              +--------------+
              | dim_products |
              +--------------+

       +-------------+            +--------------+
       | dim_channels|<--------- | fact_feedback|
       +-------------+            +--------------+

```

### Kết quả yêu cầu
- Dashboard: phân tích đơn hàng theo khu vực, nguồn người dùng, hiệu suất chiến dịch marketing
    
- Lưu vết thay đổi user, sản phẩm, phản hồi theo thời gian
    
- API dịch vụ nội bộ có thể gọi warehouse để lấy dữ liệu tích hợp
    
- Có thể mở rộng sang ML pipeline nếu muốn

### Công cụ tham khảo(tùy theo năng lực)
| Mục         | Cơ bản          | Nâng cao            |
| ----------- | --------------- | ------------------- |
| Extract     | Airbyte, Python | Kafka CDC, Debezium |
| Load        | PostgreSQL      | Snowflake, BigQuery |
| Transform   | dbt, Pandas     | PySpark             |
| Orchestrate | Airflow         | Airflow, Prefect    |
| Serve       | Metabase        | Superset, Looker    |
### Cấu trúc module dự án
| Module           | Nội dung                                                      |
| ---------------- | ------------------------------------------------------------- |
| `extractors/`    | Python script hoặc cấu hình Airbyte cho từng nguồn            |
| `transform/`     | dbt project với các model `fact_*`, `dim_*`, `intermediate_*` |
| `validation/`    | Script hoặc dbt test kiểm tra chất lượng dữ liệu              |
| `orchestration/` | DAG Airflow để tự động hóa toàn bộ pipeline                   |
| `docs/`          | Metadata catalog, lineage, mô tả bảng                         |
| `bi_dashboards/` | Dashboard Metabase / Superset mẫu để trình bày kết quả        |
