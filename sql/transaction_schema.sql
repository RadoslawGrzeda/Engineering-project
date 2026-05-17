CREATE SCHEMA IF NOT EXISTS transaction;

CREATE TABLE transaction.dict_pos (
    pos_information_id   VARCHAR(100)    NOT NULL,
    pos_name            VARCHAR(255),
    pos_version         VARCHAR(50),
    manufacturer        VARCHAR(100),
    connection_type     VARCHAR(50),
    is_contactless      BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_dict_pos PRIMARY KEY (pos_information_id)
);

CREATE TABLE transaction.dict_printer (
    printer_information_id   VARCHAR(100)    NOT NULL,
    printer_brand           VARCHAR(255),
    printer_model           VARCHAR(100),
    connection_type         VARCHAR(50),
    paper_width_mm          INT             NOT NULL DEFAULT 80,
    created_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_dict_printer PRIMARY KEY (printer_information_id)
);

-- ============================================================
-- ENTITY TABLES (SCD2 pattern)
-- ============================================================

CREATE TABLE transaction.pos (
    pos_id              VARCHAR(50)     NOT NULL,
    pos_information_id   VARCHAR(100)    NOT NULL,
    shop_id             VARCHAR(50)     NOT NULL,
    is_current          BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_pos PRIMARY KEY (pos_id),
    CONSTRAINT fk_pos_dict_pos FOREIGN KEY (pos_information_id)
        REFERENCES transaction.dict_pos (pos_information_id)
);

CREATE INDEX idx_pos_information_id ON transaction.pos (pos_information_id);
CREATE INDEX idx_pos_shop_id ON transaction.pos (shop_id);
CREATE INDEX idx_pos_is_current ON transaction.pos (is_current);

CREATE TABLE transaction.printer (
    printer_id              VARCHAR(50)     NOT NULL,
    printer_information_id   VARCHAR(100)    NOT NULL,
    shop_id                 VARCHAR(50)     NOT NULL,
    is_current              BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at     TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_printer PRIMARY KEY (printer_id),
    CONSTRAINT fk_printer_dict_printer FOREIGN KEY (printer_information_id)
        REFERENCES transaction.dict_printer (printer_information_id)
);

CREATE INDEX idx_printer_serial_number ON transaction.printer (printer_information_id);
CREATE INDEX idx_printer_shop_id ON transaction.printer (shop_id);
CREATE INDEX idx_printer_is_current ON transaction.printer (is_current);

CREATE TABLE transaction.cashier (
    cashier_id      VARCHAR(50)     NOT NULL,
    location_code   VARCHAR(50),
    person_id       VARCHAR(50),
    is_current      BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_cashier PRIMARY KEY (cashier_id),
    CONSTRAINT fk_cashier_person FOREIGN KEY (person_id)
        REFERENCES client.customer (person_id)
);

CREATE INDEX idx_cashier_location_code ON transaction.cashier (location_code);
CREATE INDEX idx_cashier_person_id ON transaction.cashier (person_id);

-- ============================================================
-- CORE TRANSACTION TABLE
-- ============================================================

CREATE TABLE transaction.transaction (
    transaction_id      VARCHAR(50)     NOT NULL,
    transaction_date                TIMESTAMP       NOT NULL,
    location_code       VARCHAR(50),
    identifier_no       VARCHAR(100),
    pos_id              VARCHAR(50),
    printer_id          VARCHAR(50),
    currency_code       VARCHAR(10),
    cashier_id          VARCHAR(50),
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    correlation_id      VARCHAR(100),

    CONSTRAINT pk_transaction PRIMARY KEY (transaction_id),
    CONSTRAINT fk_transaction_pos FOREIGN KEY (pos_id)
        REFERENCES transaction.pos (pos_id),
    CONSTRAINT fk_transaction_printer FOREIGN KEY (printer_id)
        REFERENCES transaction.printer (printer_id),
    CONSTRAINT fk_transaction_cashier FOREIGN KEY (cashier_id)
        REFERENCES transaction.cashier (cashier_id)
);

CREATE INDEX idx_transaction_date ON transaction.transaction (transaction_date);
CREATE INDEX idx_transaction_location_code ON transaction.transaction (location_code);
CREATE INDEX idx_transaction_pos_id ON transaction.transaction (pos_id);
CREATE INDEX idx_transaction_printer_id ON transaction.transaction (printer_id);
CREATE INDEX idx_transaction_cashier_id ON transaction.transaction (cashier_id);
CREATE INDEX idx_transaction_correlation_id ON transaction.transaction (correlation_id);
CREATE INDEX idx_transaction_identifier_no ON transaction.transaction (identifier_no);

-- ============================================================
-- TRANSACTION CHILD TABLES
-- ============================================================

CREATE TABLE transaction.transaction_line (
    transaction_line_id     VARCHAR(50)     NOT NULL,
    transaction_id          VARCHAR(50)     NOT NULL,
    prd_code                VARCHAR(100),
    quantity                DECIMAL(18, 2),
    unit_price_net          DECIMAL(18, 2),
    tax_rate                DECIMAL(5, 2),
    line_net_value          DECIMAL(18, 2),
    total_line_value        DECIMAL(18, 2),
    total_tax_amount        DECIMAL(18, 2),
    discount_value          DECIMAL(18, 2)  DEFAULT 0,
    created_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    correlation_id          VARCHAR(100),

    CONSTRAINT pk_transaction_line PRIMARY KEY (transaction_line_id),
    CONSTRAINT fk_tline_transaction FOREIGN KEY (transaction_id)
        REFERENCES transaction.transaction (transaction_id)
);

CREATE INDEX idx_tline_transaction_id ON transaction.transaction_line (transaction_id);
CREATE INDEX idx_tline_prd_code ON transaction.transaction_line (prd_code);
CREATE INDEX idx_tline_correlation_id ON transaction.transaction_line (correlation_id);

CREATE TABLE transaction.transaction_metadata (
    metadata_id         VARCHAR(50)     NOT NULL,
    transaction_id      VARCHAR(50)     NOT NULL,
    comments            TEXT,
    import_field        VARCHAR(255),
    shared              BOOLEAN,
    user_loan           BOOLEAN,
    created_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    correlation_id      VARCHAR(100),

    CONSTRAINT pk_transaction_metadata PRIMARY KEY (metadata_id),
    CONSTRAINT fk_tmeta_transaction FOREIGN KEY (transaction_id)
        REFERENCES transaction.transaction (transaction_id)
);

CREATE INDEX idx_tmeta_transaction_id ON transaction.transaction_metadata (transaction_id);
CREATE INDEX idx_tmeta_correlation_id ON transaction.transaction_metadata (correlation_id);

CREATE TABLE transaction.transaction_status (
    transaction_status_id   VARCHAR(50)     NOT NULL,
    transaction_id          VARCHAR(50)     NOT NULL,
    status                  VARCHAR(50),
    cancelled               BOOLEAN         DEFAULT FALSE,
    payment_status          VARCHAR(50),
    transaction_status      VARCHAR(50),
    is_current              BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    correlation_id          VARCHAR(100),

    CONSTRAINT pk_transaction_status PRIMARY KEY (transaction_status_id),
    CONSTRAINT fk_tstatus_transaction FOREIGN KEY (transaction_id)
        REFERENCES transaction.transaction (transaction_id)
);

CREATE INDEX idx_tstatus_transaction_id ON transaction.transaction_status (transaction_id);
CREATE INDEX idx_tstatus_is_current ON transaction.transaction_status (is_current);
CREATE INDEX idx_tstatus_status ON transaction.transaction_status (status);
CREATE INDEX idx_tstatus_payment_status ON transaction.transaction_status (payment_status);
CREATE INDEX idx_tstatus_correlation_id ON transaction.transaction_status (correlation_id);

CREATE TABLE transaction.transaction_payment (
    payment_id          VARCHAR(50)     NOT NULL,
    transaction_id      VARCHAR(50)     NOT NULL,
    method              VARCHAR(50),
    total_value         DECIMAL(18, 2),
    total_net_value     DECIMAL(18, 2),
    total_payment       DECIMAL(18, 2),
    discount_value      DECIMAL(18, 2),
    created_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    correlation_id      VARCHAR(100),

    CONSTRAINT pk_transaction_payment PRIMARY KEY (payment_id),
    CONSTRAINT fk_tpayment_transaction FOREIGN KEY (transaction_id)
        REFERENCES transaction.transaction (transaction_id)
);

CREATE INDEX idx_tpayment_transaction_id ON transaction.transaction_payment (transaction_id);
CREATE INDEX idx_tpayment_method ON transaction.transaction_payment (method);
CREATE INDEX idx_tpayment_correlation_id ON transaction.transaction_payment (correlation_id);

-- ============================================================
-- DEAD LETTER TABLE
-- ============================================================

CREATE TABLE transaction.dead_letter
(
    id                 SERIAL PRIMARY KEY,
    inserted_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_code         VARCHAR(100),
    error_message      TEXT,
--     retry_count        INTEGER                  DEFAULT 0,
--     status             VARCHAR(20)              DEFAULT 'NEW'
--         CONSTRAINT check_status
--             CHECK (status IN ('NEW', 'RETRIED', 'RESOLVED', 'IGNORED')),
    transaction_id     VARCHAR(50),
    correlation_id     VARCHAR(100),
    transaction_date   TIMESTAMP,
    location_code      VARCHAR(50),
    raw_payload        JSONB NOT NULL
);

CREATE INDEX idx_tx_dlq_transaction_id ON transaction.dead_letter (transaction_id);
CREATE INDEX idx_tx_dlq_correlation_id ON transaction.dead_letter (correlation_id);
-- CREATE INDEX idx_tx_dlq_status ON transaction.dead_letter (status);
CREATE INDEX idx_tx_dlq_inserted_at ON transaction.dead_letter (inserted_at);
CREATE INDEX idx_tx_dlq_location_code ON transaction.dead_letter (location_code);
CREATE INDEX idx_tx_dlq_raw_payload ON transaction.dead_letter USING GIN (raw_payload);