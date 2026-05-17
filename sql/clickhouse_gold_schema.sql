CREATE DATABASE IF NOT EXISTS marts_transaction;

CREATE TABLE IF NOT EXISTS marts_transaction.fact_transactions
(
    transaction_id      String,
    transaction_date    DateTime,
    location_code       String,
    identifier_no       Nullable(String),
    pos_id              String,
    cashier_id          String,
    currency_code       String,

    payment_method      Nullable(String),
    total_net_value     Decimal(18, 4),
    total_gross_value   Decimal(18, 4),
    discount_value      Decimal(18, 4),

    status              Nullable(String),
    payment_status      Nullable(String),
    cancelled           Bool,

    lines Nested (
        prd_code         String,
        quantity         Int32,
        unit_price_net   Decimal(18, 4),
        tax_rate         Decimal(18, 4),
        line_net_value   Decimal(18, 4),
        total_line_value Decimal(18, 4),
        tax_amount       Decimal(18, 4),
        discount_value   Decimal(18, 4)
    ),

    correlation_id      String,
    _ingested_at        DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_ingested_at)
PARTITION BY toYYYYMM(transaction_date)
ORDER BY (transaction_id)
SETTINGS index_granularity = 8192;
