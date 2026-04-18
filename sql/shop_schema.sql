CREATE SCHEMA IF NOT EXISTS store;

CREATE TABLE store.site (
    site_unique_code VARCHAR(5) NOT NULL PRIMARY KEY,
    site_code VARCHAR(100) NOT NULL,
    site_name VARCHAR(100) NOT NULL CHECK (length(site_name) >= 1),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(255),

    CONSTRAINT chk_site_unique_code CHECK (site_unique_code ~ '^PL[0-9]{3}$')
);

CREATE INDEX idx_site_site_unique_code ON store.site(site_unique_code);
CREATE INDEX idx_site_site_code ON store.site(site_code);

CREATE TABLE store.site_info (
    site_info_id SERIAL PRIMARY KEY,
    site_unique_code Varchar(100) NOT NULL REFERENCES store.site(site_unique_code),
    site_status_code VARCHAR(100) NOT NULL,
    site_opening_date DATE,
    site_closing_date DATE,
    is_current BOOLEAN,
--     valid_from DATE,
--     valid_to DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(255),
    CONSTRAINT chk_site_status_code CHECK (site_status_code IN ('ACTIVE', 'CLOSED'))
);

CREATE INDEX idx_site_info_site_unique_code ON store.site_info(site_unique_code);
CREATE INDEX idx_site_info_status ON store.site_info(site_status_code);

CREATE TABLE store.site_format (
    site_format_id SERIAL PRIMARY KEY,
    site_unique_code VARCHAR(100) NOT NULL REFERENCES store.site(site_unique_code),
    site_format_unique_code VARCHAR(10) NOT NULL,
    is_current BOOLEAN,
    valid_from DATE,
    valid_to DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(255),

    CONSTRAINT chk_site_format_code CHECK (site_format_unique_code IN ('HIPER', 'SUPER'))
);

CREATE INDEX idx_site_format_site_unique_code ON store.site_format(site_unique_code);
CREATE INDEX idx_site_format_code ON store.site_format(site_format_unique_code);

CREATE TABLE store.site_address (
    site_address_id SERIAL PRIMARY KEY,
    site_unique_code VARCHAR(100) NOT NULL REFERENCES store.site(site_unique_code),
    site_address_zip_code VARCHAR(6) NOT NULL,
    site_address_city VARCHAR(100) NOT NULL,
    site_address_street VARCHAR(255) NOT NULL,
    city_code VARCHAR(3) NOT NULL,
    country_code VARCHAR(2) NOT NULL,
    site_geo_coordinate_x_value NUMERIC(12, 9),
    site_geo_coordinate_y_value NUMERIC(12, 9),
    is_current BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(255),

    CONSTRAINT chk_zip_code CHECK (site_address_zip_code ~ '^[0-9]{2}-[0-9]{3}$'),
    CONSTRAINT chk_city_code CHECK (city_code ~ '^[A-Z0-9]{3}$'),
    CONSTRAINT chk_country_code CHECK (country_code ~ '^[A-Z]{2}$'),
    CONSTRAINT chk_latitude CHECK (site_geo_coordinate_x_value BETWEEN 49.0 AND 55.0),
    CONSTRAINT chk_longitude CHECK (site_geo_coordinate_y_value BETWEEN 14.0 AND 24.5)
);

CREATE INDEX idx_site_address_site_unique_code ON store.site_address(site_unique_code);
CREATE INDEX idx_site_address_city ON store.site_address(site_address_city);
CREATE INDEX idx_site_address_city_code ON store.site_address(city_code);

CREATE TABLE store.site_contact (
    site_contact_id SERIAL PRIMARY KEY,
    site_unique_code VARCHAR(100) NOT NULL REFERENCES store.site(site_unique_code),
    contact_type VARCHAR(50) NOT NULL,
    contact_value VARCHAR(255) NOT NULL,
    contact_role VARCHAR(100) NOT NULL,
    valid_from DATE,
    valid_to DATE,
    is_primary BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(255)
);

CREATE INDEX idx_site_contact_site_unique_code ON store.site_contact(site_unique_code);
CREATE INDEX idx_site_contact_type ON store.site_contact(contact_type);
CREATE INDEX idx_site_contact_role ON store.site_contact(contact_role);

CREATE TABLE store.etl_load_log (
    id                   SERIAL PRIMARY KEY,
    user_name              VARCHAR(50),
    destination_table    VARCHAR(50) NOT NULL,
    file_name            VARCHAR(50) NOT NULL,
    number_of_rows       INT,
    file_size            BIGINT,
    rejected_rows_count  INT DEFAULT 0,
    inserted_rows_count  INT DEFAULT 0,
    created_by           VARCHAR(50),
    correlation_id       VARCHAR(8),
    status               VARCHAR(50) CHECK (status IN ('success','partial_success', 'error', 'pending')),
    error_message        TEXT default null,
    processed_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_etl_load_log_correlation_id ON store.etl_load_log(correlation_id);
CREATE INDEX idx_etl_load_log_status ON store.etl_load_log(status);
CREATE INDEX idx_etl_load_log_destination_table ON store.etl_load_log(destination_table);

CREATE TABLE store.dead_letter (
    id              SERIAL PRIMARY KEY,
    source_table    VARCHAR(50),
    source_file     TEXT,
    raw_row         JSONB,
    error_details   TEXT,
    line_no         INT,
    correlation_id  VARCHAR(8),
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_dead_letter_correlation_id ON store.dead_letter(correlation_id);
CREATE INDEX idx_dead_letter_source_table ON store.dead_letter(source_table);