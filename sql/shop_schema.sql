CREATE SCHEMA IF NOT EXISTS store;

CREATE TABLE store.site (
    site_unique_code VARCHAR(5)  NOT NULL PRIMARY KEY,
    site_code        VARCHAR(100) NOT NULL,
    site_name        VARCHAR(100) NOT NULL CHECK (length(site_name) >= 1),
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file      VARCHAR(255),

    CONSTRAINT chk_site_unique_code CHECK (site_unique_code ~ '^PL[0-9]{3}$')
);

CREATE INDEX idx_site_site_code ON store.site(site_code);

CREATE TABLE store.site_info (
    site_unique_code  VARCHAR(5)   NOT NULL PRIMARY KEY REFERENCES store.site(site_unique_code),
    site_status_code  VARCHAR(100) NOT NULL,
    site_opening_date DATE,
    site_closing_date DATE,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file       VARCHAR(255),

    CONSTRAINT chk_site_status_code CHECK (site_status_code IN ('ACTIVE', 'CLOSED'))
);

CREATE INDEX idx_site_info_status ON store.site_info(site_status_code);

CREATE TABLE store.site_format (
    site_unique_code       VARCHAR(5)  NOT NULL PRIMARY KEY REFERENCES store.site(site_unique_code),
    site_format_unique_code VARCHAR(10) NOT NULL,
    created_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file            VARCHAR(255),

    CONSTRAINT chk_site_format_code CHECK (site_format_unique_code IN ('HIPER', 'SUPER'))
);

CREATE INDEX idx_site_format_code ON store.site_format(site_format_unique_code);

CREATE TABLE store.site_address (
    site_unique_code             VARCHAR(5)     NOT NULL PRIMARY KEY REFERENCES store.site(site_unique_code),
    site_address_zip_code        VARCHAR(6)     NOT NULL,
    site_address_city            VARCHAR(100)   NOT NULL,
    site_address_street          VARCHAR(255)   NOT NULL,
    city_code                    VARCHAR(3)     NOT NULL,
    country_code                 VARCHAR(2)     NOT NULL,
    site_geo_coordinate_x_value  NUMERIC(12, 9),
    site_geo_coordinate_y_value  NUMERIC(12, 9),
    created_at                   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at                   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file                  VARCHAR(255),

    CONSTRAINT chk_zip_code   CHECK (site_address_zip_code ~ '^[0-9]{2}-[0-9]{3}$'),
    CONSTRAINT chk_city_code  CHECK (city_code ~ '^[A-Z0-9]{3}$'),
    CONSTRAINT chk_country_code CHECK (country_code ~ '^[A-Z]{2}$'),
    CONSTRAINT chk_latitude   CHECK (site_geo_coordinate_x_value BETWEEN 49.0 AND 55.0),
    CONSTRAINT chk_longitude  CHECK (site_geo_coordinate_y_value BETWEEN 14.0 AND 24.5)
);

CREATE INDEX idx_site_address_city      ON store.site_address(site_address_city);
CREATE INDEX idx_site_address_city_code ON store.site_address(city_code);

CREATE TABLE store.site_contact (
    site_contact_id  SERIAL       PRIMARY KEY,
    site_unique_code VARCHAR(5)   NOT NULL REFERENCES store.site(site_unique_code),
    contact_type     VARCHAR(50)  NOT NULL,
    contact_value    VARCHAR(255) NOT NULL,
    contact_role     VARCHAR(100) NOT NULL,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file      VARCHAR(255),

    CONSTRAINT uq_site_contact UNIQUE (site_unique_code, contact_type, contact_role)
);

CREATE INDEX idx_site_contact_site_unique_code ON store.site_contact(site_unique_code);
CREATE INDEX idx_site_contact_type             ON store.site_contact(contact_type);
CREATE INDEX idx_site_contact_role             ON store.site_contact(contact_role);

CREATE OR REPLACE FUNCTION store.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_site_updated_at
    BEFORE UPDATE ON store.site
    FOR EACH ROW EXECUTE FUNCTION store.set_updated_at();

CREATE TRIGGER trg_site_info_updated_at
    BEFORE UPDATE ON store.site_info
    FOR EACH ROW EXECUTE FUNCTION store.set_updated_at();

CREATE TRIGGER trg_site_format_updated_at
    BEFORE UPDATE ON store.site_format
    FOR EACH ROW EXECUTE FUNCTION store.set_updated_at();

CREATE TRIGGER trg_site_address_updated_at
    BEFORE UPDATE ON store.site_address
    FOR EACH ROW EXECUTE FUNCTION store.set_updated_at();

CREATE TRIGGER trg_site_contact_updated_at
    BEFORE UPDATE ON store.site_contact
    FOR EACH ROW EXECUTE FUNCTION store.set_updated_at();

