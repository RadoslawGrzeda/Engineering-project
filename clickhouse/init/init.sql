CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.fact_transactions
(
    transaction_id    String,
    transaction_date  DateTime64(6, 'UTC'),
    location_code     LowCardinality(String),
    identifier_no     Nullable(String) ,
    pos_id            LowCardinality(String),
    cashier_id        LowCardinality(String),
    currency_code     LowCardinality(String),

    payment_method    LowCardinality(String),
    total_net_value   Decimal64(2),
    total_gross_value Decimal64(2),
    discount_value    Decimal64(2),

    status            LowCardinality(String),
    payment_status    LowCardinality(String),
    cancelled         Bool DEFAULT false,

    lines Nested
    (
        prd_code         String,
        quantity         Int32,
        unit_price_net   Decimal64(2),
        tax_rate         Decimal(5, 2),
        line_net_value   Decimal64(2),
        total_line_value Decimal64(2),
        tax_amount       Decimal64(2),
        discount_value   Decimal64(2)
    ),

    correlation_id    String,
    ingested_at       DateTime64(6, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(transaction_date)
ORDER BY (location_code, transaction_date, transaction_id)
SETTINGS index_granularity = 8192;
