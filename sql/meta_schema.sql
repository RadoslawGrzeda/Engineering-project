CREATE SCHEMA IF NOT EXISTS meta;

CREATE OR REPLACE FUNCTION meta.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE TABLE meta.product_etl_load_log (
    id                   SERIAL PRIMARY KEY,
    user_name            VARCHAR(100),
    destination_table    VARCHAR(100) NOT NULL,
    file_name            VARCHAR(255) NOT NULL,
    number_of_rows       INT,
    file_size            BIGINT,
    rejected_rows_count  INT DEFAULT 0,
    inserted_rows_count  INT DEFAULT 0,
    created_by           VARCHAR(100),
    correlation_id       VARCHAR(50),
    status               VARCHAR(50) CHECK (status IN ('success', 'partial_success', 'error', 'pending')),
    error_message        TEXT DEFAULT NULL,
    processed_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_product_etl_log_correlation_id    ON meta.product_etl_load_log(correlation_id);
CREATE INDEX idx_product_etl_log_status            ON meta.product_etl_load_log(status);
CREATE INDEX idx_product_etl_log_destination_table ON meta.product_etl_load_log(destination_table);

CREATE TABLE meta.store_etl_load_log (
    id                   SERIAL PRIMARY KEY,
    user_name            VARCHAR(100),
    destination_table    VARCHAR(100) NOT NULL,
    file_name            VARCHAR(255) NOT NULL,
    number_of_rows       INT,
    file_size            BIGINT,
    rejected_rows_count  INT DEFAULT 0,
    inserted_rows_count  INT DEFAULT 0,
    created_by           VARCHAR(100),
    correlation_id       VARCHAR(50),
    status               VARCHAR(50) CHECK (status IN ('success', 'partial_success', 'error', 'pending')),
    error_message        TEXT DEFAULT NULL,
    processed_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_store_etl_log_correlation_id    ON meta.store_etl_load_log(correlation_id);
CREATE INDEX idx_store_etl_log_status            ON meta.store_etl_load_log(status);
CREATE INDEX idx_store_etl_log_destination_table ON meta.store_etl_load_log(destination_table);


CREATE TABLE meta.product_dead_letter (
    id             SERIAL PRIMARY KEY,
    source_table   VARCHAR(50),
    source_file    TEXT,
    raw_row        JSONB,
    error_details  TEXT,
    line_no        INT,
    correlation_id VARCHAR(50),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_product_dead_letter_correlation_id ON meta.product_dead_letter(correlation_id);
CREATE INDEX idx_product_dead_letter_source_table   ON meta.product_dead_letter(source_table);

CREATE TRIGGER trg_product_dead_letter_updated_at
    BEFORE UPDATE ON meta.product_dead_letter
    FOR EACH ROW EXECUTE FUNCTION meta.set_updated_at();

CREATE TABLE meta.store_dead_letter (
    id             SERIAL PRIMARY KEY,
    source_table   VARCHAR(50),
    source_file    TEXT,
    raw_row        JSONB,
    error_details  TEXT,
    line_no        INT,
    correlation_id VARCHAR(50),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_store_dead_letter_correlation_id ON meta.store_dead_letter(correlation_id);
CREATE INDEX idx_store_dead_letter_source_table   ON meta.store_dead_letter(source_table);

CREATE TRIGGER trg_store_dead_letter_updated_at
    BEFORE UPDATE ON meta.store_dead_letter
    FOR EACH ROW EXECUTE FUNCTION meta.set_updated_at();

CREATE TABLE meta.bronze_watermark (
    dag_id       VARCHAR(100) PRIMARY KEY,
    last_loaded  TIMESTAMP NOT NULL DEFAULT '1970-01-01',
    created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER trg_bronze_watermark_updated_at
    BEFORE UPDATE ON meta.bronze_watermark
    FOR EACH ROW EXECUTE FUNCTION meta.set_updated_at();



CREATE TABLE meta.client_dead_letter
(
    id                 SERIAL PRIMARY KEY,
    inserted_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_code         VARCHAR(100),
    error_message      TEXT,
    retry_count        INTEGER                  DEFAULT 0,
    status             VARCHAR(20)              DEFAULT 'NEW'
        CONSTRAINT check_status
            CHECK (status IN ('NEW', 'RETRIED', 'RESOLVED', 'IGNORED')),
    person_id          VARCHAR(12),
    correlation_id     VARCHAR(100),
    source_application VARCHAR(50),
    raw_payload        JSONB NOT NULL,
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_cdl_dedup UNIQUE (person_id, correlation_id, error_code)
);

CREATE INDEX idx_client_dead_letter_person_id ON meta.client_dead_letter (person_id);
CREATE INDEX idx_client_dead_letter_correlation_id ON meta.client_dead_letter (correlation_id);
CREATE INDEX idx_client_dead_letter_status ON meta.client_dead_letter (status);
CREATE INDEX idx_client_dead_letter_inserted_at ON meta.client_dead_letter (inserted_at);
CREATE INDEX idx_client_dead_letter_raw_payload ON meta.client_dead_letter USING GIN (raw_payload);

CREATE TRIGGER trg_client_dead_letter_updated_at
    BEFORE UPDATE ON meta.client_dead_letter
    FOR EACH ROW EXECUTE FUNCTION meta.set_updated_at();