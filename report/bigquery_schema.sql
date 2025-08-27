CREATE OR REPLACE TABLE analytics.users
PARTITION BY DATE(signup_date)
CLUSTER BY user_id AS
SELECT
  CAST(NULL AS STRING) AS user_id,
  CAST(NULL AS STRING) AS source,
  CAST(NULL AS STRING) AS name,
  CAST(NULL AS STRING) AS email,
  CAST(NULL AS STRING) AS phone,
  CAST(NULL AS STRING) AS gender,
  CAST(NULL AS DATE) AS dob,
  CAST(NULL AS STRING) AS country,
  CAST(NULL AS STRING) AS city,
  CAST(NULL AS STRING) AS timezone,
  CAST(NULL AS DATE) AS signup_date,
  CAST(NULL AS TIMESTAMP) AS last_login,
  CAST(NULL AS BOOL) AS is_active,
  CAST(NULL AS STRING) AS customer_tier,
  CAST(NULL AS NUMERIC) AS total_spent,
  CAST(NULL AS STRING) AS preferred_language,
  CAST(NULL AS BOOL) AS marketing_consent,
  CAST(NULL AS STRING) AS address,
  ARRAY<STRUCT<
    session_id STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    device_type STRING,
    browser STRING,
    os STRING,
    ip_address STRING,
    location_country STRING,
    location_city STRING,
    total_interactions INT64,
    pages_visited INT64,
    conversion_event STRING,
    exit_page STRING,
    session_duration_minutes INT64
  >>[] AS sessions,
  ARRAY<STRUCT<
    category STRING,
    preference_type STRING,
    preference_value STRING,
    confidence_score FLOAT64,
    last_updated TIMESTAMP,
    source STRING,
    is_explicit BOOL
  >>[] AS preferences,
  ARRAY<STRUCT<
    notification_id STRING,
    type STRING,
    title STRING,
    content STRING,
    sent_at TIMESTAMP,
    read_at TIMESTAMP,
    clicked_at TIMESTAMP,
    channel STRING,
    priority STRING,
    category STRING,
    metadata JSON
  >>[] AS notifications
WHERE FALSE;


CREATE OR REPLACE TABLE analytics.orders
PARTITION BY DATE(order_date)
CLUSTER BY user_id AS
SELECT
  CAST(NULL AS STRING) AS order_id,
  CAST(NULL AS STRING) AS user_id,
  CAST(NULL AS TIMESTAMP) AS order_date,
  CAST(NULL AS STRING) AS status,
  CAST(NULL AS NUMERIC) AS total_amount,
  CAST(NULL AS NUMERIC) AS shipping_fee,
  CAST(NULL AS NUMERIC) AS tax_amount,
  CAST(NULL AS NUMERIC) AS discount_amount,
  CAST(NULL AS STRING) AS payment_status,
  CAST(NULL AS STRING) AS currency,
  CAST(NULL AS STRING) AS shipping_address,
  CAST(NULL AS STRING) AS notes,
  ARRAY<STRUCT<
    item_id STRING,
    product_id STRING,
    product_name STRING,
    category STRING,
    brand STRING,
    quantity INT64,
    unit_price NUMERIC,
    discount_per_item NUMERIC,
    total_price NUMERIC
  >>[] AS items,
  ARRAY<STRUCT<
    transaction_id STRING,
    amount NUMERIC,
    method STRING,
    status STRING,
    timestamp TIMESTAMP,
    gateway_response JSON,
    reference_number STRING,
    fee NUMERIC
  >>[] AS transactions
WHERE FALSE;


CREATE OR REPLACE TABLE analytics.events
PARTITION BY DATE(timestamp)
CLUSTER BY user_id, event_type AS
SELECT
  CAST(NULL AS STRING) AS event_id,
  CAST(NULL AS STRING) AS user_id,
  CAST(NULL AS STRING) AS session_id,
  CAST(NULL AS STRING) AS event_type,
  CAST(NULL AS STRING) AS target_id,
  CAST(NULL AS TIMESTAMP) AS timestamp,
  CAST(NULL AS INT64) AS duration_seconds,
  CAST(NULL AS BOOL) AS success,
  CAST(NULL AS STRING) AS device_type,
  CAST(NULL AS STRING) AS browser,
  CAST(NULL AS STRING) AS os,
  CAST(NULL AS STRING) AS ip_address,
  CAST(NULL AS STRING) AS user_agent,
  CAST(NULL AS STRING) AS referrer,
  CAST(NULL AS STRING) AS page_url,
  CAST(NULL AS JSON) AS metadata
WHERE FALSE;


CREATE OR REPLACE TABLE analytics.messages
PARTITION BY DATE(created_at)
CLUSTER BY thread_id AS
SELECT
  CAST(NULL AS STRING) AS thread_id,
  CAST(NULL AS STRING) AS user_id,
  CAST(NULL AS STRING) AS conversation_type,
  CAST(NULL AS STRING) AS status,
  CAST(NULL AS STRING) AS priority,
  CAST(NULL AS STRING) AS assigned_agent,
  CAST(NULL AS TIMESTAMP) AS created_at,
  CAST(NULL AS TIMESTAMP) AS updated_at,
  CAST(NULL AS INT64) AS satisfaction_rating,
  CAST(NULL AS BOOL) AS resolved,
  CAST(NULL AS INT64) AS resolution_time_minutes,
  ARRAY<STRING>[] AS tags,
  ARRAY<STRUCT<
    message_id STRING,
    sender STRING,
    sender_type STRING,
    receiver STRING,
    timestamp TIMESTAMP,
    content STRING,
    message_type STRING,
    priority STRING,
    channel STRING,
    attachments ARRAY<STRUCT<
      filename STRING,
      url STRING,
      size INT64
    >>,
    internal_notes STRING
  >>[] AS messages
WHERE FALSE;


CREATE OR REPLACE TABLE analytics.products
PARTITION BY DATE(updated_at)
CLUSTER BY product_id AS
SELECT
  CAST(NULL AS STRING) AS product_id,
  CAST(NULL AS STRING) AS source,
  CAST(NULL AS STRING) AS name,
  CAST(NULL AS STRING) AS category,
  CAST(NULL AS STRING) AS brand,
  CAST(NULL AS NUMERIC) AS price,
  CAST(NULL AS STRING) AS description,
  CAST(NULL AS BOOL) AS available,
  CAST(NULL AS TIMESTAMP) AS created_at,
  CAST(NULL AS TIMESTAMP) AS updated_at,
  CAST(NULL AS BOOL) AS is_active,
  CAST(NULL AS STRING) AS status,
  CAST(NULL AS BOOL) AS featured,
  CAST(NULL AS INT64) AS sales_count,
  CAST(NULL AS INT64) AS view_count,
  ARRAY<STRING>[] AS tags,
  CAST(NULL AS JSON) AS specs,
  ARRAY<STRUCT<
    sku STRING,
    color STRING,
    size STRING,
    price_diff NUMERIC,
    stock INT64
  >>[] AS variants,
  ARRAY<STRING>[] AS images,
  STRUCT<
    title STRING,
    description STRING,
    keywords ARRAY<STRING>
  >(NULL, NULL, []) AS seo,
  STRUCT<
    total_stock INT64,
    reserved INT64,
    safety_stock INT64
  >(NULL, NULL, NULL) AS inventory,
  ARRAY<STRUCT<
    warehouse STRING,
    stock INT64,
    reserved INT64,
    last_updated TIMESTAMP,
    reorder_point INT64,
    max_capacity INT64,
    location STRING,
    temperature_controlled BOOL,
    manager STRING
  >>[] AS stock_levels,
  ARRAY<STRUCT<
    shipment_id STRING,
    quantity INT64,
    expected_date TIMESTAMP,
    supplier STRING,
    status STRING
  >>[] AS incoming_shipments,
  ARRAY<STRUCT<
    type STRING,
    warehouse STRING,
    quantity INT64,
    created_at TIMESTAMP,
    resolved BOOL
  >>[] AS alerts,
  ARRAY<STRUCT<
    rating_id STRING,
    user_id STRING,
    rating INT64,
    verified_purchase BOOL,
    tags ARRAY<STRING>,
    comment STRING,
    helpful_votes INT64,
    total_votes INT64,
    timestamp TIMESTAMP,
    updated_at TIMESTAMP,
    status STRING,
    language STRING,
    device_type STRING,
    location STRUCT<country STRING, city STRING>,
    images ARRAY<STRING>,
    response JSON
  >>[] AS ratings
WHERE FALSE;


CREATE OR REPLACE TABLE analytics.fact_user_orders
PARTITION BY order_date
CLUSTER BY user_id AS
SELECT
  CAST(NULL AS STRING) AS user_id,
  CAST(NULL AS STRING) AS order_id,
  CAST(NULL AS DATE) AS order_date,
  CAST(NULL AS STRING) AS customer_tier,
  CAST(NULL AS STRING) AS country,
  CAST(NULL AS NUMERIC) AS total_amount,
  CAST(NULL AS INT64) AS product_count,
  CAST(NULL AS STRING) AS payment_status
WHERE FALSE;
