CREATE SCHEMA IF NOT EXISTS client;

CREATE TABLE client.dict_gender
(
    gender_code VARCHAR(10) PRIMARY KEY,
    gender_name VARCHAR(50) NOT NULL,
    created_at  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE client.dict_country
(
    country_code        VARCHAR(3) PRIMARY KEY,
    country_name        VARCHAR(100) NOT NULL,
    number_of_neighbors SMALLINT,
    access_to_the_sea   BOOLEAN,
    population          BIGINT,
    created_at          TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE client.dict_language
(
    language_code           VARCHAR(10) PRIMARY KEY,
    language_name           VARCHAR(100) NOT NULL,
    language_min_level_code VARCHAR(10),
    language_max_level_code VARCHAR(10),
    created_at              TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE client.dict_loyalty_status
(
    status_code  VARCHAR(20) PRIMARY KEY,
    status_name  VARCHAR(100) NOT NULL,
    status_rules TEXT,
    created_at   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE client.dict_indicator
(
    indicator_type        VARCHAR(50) PRIMARY KEY,
    indicator_description VARCHAR(255),
    indicator_rules       TEXT,
    created_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE client.dict_civil
(
    civil_status_type        VARCHAR(50) PRIMARY KEY,
    civil_status_description VARCHAR(255),
    created_at               TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE client.dict_contact
(
    contact_type        VARCHAR(50) PRIMARY KEY,
    contact_name        VARCHAR(100) NOT NULL,
    contact_description VARCHAR(255),
    created_at          TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE client.dict_subscription
(
    communication_code        VARCHAR(50) PRIMARY KEY,
    communication_name        VARCHAR(100) NOT NULL,
    communication_description VARCHAR(255),
    created_at                TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE client.country_language
(
    country_code  VARCHAR(3)  NOT NULL,
    language_code VARCHAR(10) NOT NULL,
    created_at    TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (country_code, language_code),

    CONSTRAINT fk_cl_country FOREIGN KEY (country_code) REFERENCES client.dict_country (country_code),
    CONSTRAINT fk_cl_language FOREIGN KEY (language_code) REFERENCES client.dict_language (language_code)
);

-- =========================
-- TABELA GŁÓWNA
-- =========================

CREATE TABLE client.customer
(
    person_id            VARCHAR(12) PRIMARY KEY,
    first_name           VARCHAR(100) NOT NULL,
    middle_name          VARCHAR(100),
    last_name            VARCHAR(100) NOT NULL,
    birth_date           DATE,
    passport_number      VARCHAR(20),
    gender_code          VARCHAR(10),
    civil_status_code    VARCHAR(10),
    registration_date    TIMESTAMP    NOT NULL,
    creation_application VARCHAR(50),
    created_at           TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    correlation_id       VARCHAR(100),

    CONSTRAINT fk_customer_gender FOREIGN KEY (gender_code) REFERENCES client.dict_gender (gender_code),
    CONSTRAINT fk_customer_civil_staus FOREIGN KEY (civil_status_code) REFERENCES client.dict_civil (civil_status_type)

);

CREATE TABLE client.loyalty_status
(
    id              SERIAL PRIMARY KEY,
    identifier_id   VARCHAR(12) NOT NULL,
    person_id       VARCHAR(12) NOT NULL,
    status_code     VARCHAR(20) NOT NULL,
    is_current      BOOLEAN     NOT NULL DEFAULT TRUE,
    start_date      DATE        NOT NULL,
    end_date        DATE,
    evaluation_date DATE,
    correlation_id  VARCHAR(100),

    CONSTRAINT fk_ls_customer FOREIGN KEY (person_id) REFERENCES client.customer (person_id),
    CONSTRAINT fk_ls_status FOREIGN KEY (status_code) REFERENCES client.dict_loyalty_status (status_code),
    CONSTRAINT uq_loyalty_status_customer UNIQUE (identifier_id,person_id, status_code,start_date)
);

CREATE TABLE client.language
(
    id             SERIAL PRIMARY KEY,
    person_id      VARCHAR(12) NOT NULL,
    language_code  VARCHAR(10) NOT NULL,
    language_level VARCHAR(10),
    created_at     TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    correlation_id VARCHAR(100),

    CONSTRAINT fk_lang_customer FOREIGN KEY (person_id) REFERENCES client.customer (person_id),
    CONSTRAINT fk_lang_language FOREIGN KEY (language_code) REFERENCES client.dict_language (language_code),
    CONSTRAINT uq_language_person UNIQUE (person_id, language_code)
);

CREATE TABLE client.nationality
(
    id             SERIAL PRIMARY KEY,
    person_id      VARCHAR(12) NOT NULL,
    country_code   VARCHAR(3)  NOT NULL,
    created_at     TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    correlation_id VARCHAR(100),

    CONSTRAINT fk_nat_customer FOREIGN KEY (person_id) REFERENCES client.customer (person_id),
    CONSTRAINT fk_nat_country FOREIGN KEY (country_code) REFERENCES client.dict_country (country_code),
    CONSTRAINT uq_nationality_person UNIQUE (person_id, country_code)
);

CREATE TABLE client.customer_indicator
(
    id             SERIAL PRIMARY KEY,
    person_id      VARCHAR(12) NOT NULL,
    type           VARCHAR(50) NOT NULL,
    is_active      BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at     TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    correlation_id VARCHAR(100),

    CONSTRAINT fk_ci_customer FOREIGN KEY (person_id) REFERENCES client.customer (person_id),
    CONSTRAINT fk_ci_indicator FOREIGN KEY (type) REFERENCES client.dict_indicator (indicator_type),
    CONSTRAINT uq_ci_person UNIQUE (person_id, type)

);

CREATE TABLE client.address
(
    id               SERIAL PRIMARY KEY,
    person_id        VARCHAR(12) NOT NULL,
    address_type     VARCHAR(50),
    option_channel   BOOLEAN              DEFAULT TRUE,
    address_street   VARCHAR(200),
    address_zip_code VARCHAR(20),
    address_city     VARCHAR(100),
    country_code     VARCHAR(3),
    geo_coordinates_x_value NUMERIC,
    geo_coordinates_y_value NUMERIC,
    is_current       BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at       TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    correlation_id   VARCHAR(100),

    CONSTRAINT fk_addr_customer FOREIGN KEY (person_id) REFERENCES client.customer (person_id),
    CONSTRAINT fk_addr_country FOREIGN KEY (country_code) REFERENCES client.dict_country (country_code),
    CONSTRAINT uq_address_person UNIQUE (person_id, address_type)
);

CREATE TABLE client.contact
(
    id                SERIAL PRIMARY KEY,
    person_id         VARCHAR(12)  NOT NULL,
    contact_type      VARCHAR(50)  NOT NULL,
    value             VARCHAR(255) NOT NULL,
    flag_main_type    BOOLEAN               DEFAULT FALSE,
    preferred_channel BOOLEAN               DEFAULT FALSE,
    option_channel    BOOLEAN               DEFAULT TRUE,
    flag_valid        BOOLEAN               DEFAULT TRUE,
    created_at        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    correlation_id    VARCHAR(100),

    CONSTRAINT fk_cont_customer FOREIGN KEY (person_id) REFERENCES client.customer (person_id),
    CONSTRAINT fk_cont_type FOREIGN KEY (contact_type) REFERENCES client.dict_contact (contact_type),
    CONSTRAINT uq_contact_person UNIQUE (person_id, contact_type, value)
);


CREATE TABLE client.communication_subscription
(
    id                       SERIAL PRIMARY KEY,
    person_id                VARCHAR(12) NOT NULL,
    communication_code       VARCHAR(50) NOT NULL,
    value                    VARCHAR(255),
    date_of_subscription     TIMESTAMP,
    date_of_unsubscription   TIMESTAMP,
    reason_of_unsubscription VARCHAR(255),
    created_at               TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at               TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    correlation_id           VARCHAR(100),

    CONSTRAINT fk_cs_customer FOREIGN KEY (person_id) REFERENCES client.customer (person_id),
    CONSTRAINT fk_cs_subscription FOREIGN KEY (communication_code) REFERENCES client.dict_subscription (communication_code),
    CONSTRAINT uq_cs_person UNIQUE (person_id, communication_code)
);

CREATE TABLE client.digital_access
(
    id                            SERIAL PRIMARY KEY,
    person_id                     VARCHAR(12) NOT NULL,
    username                      VARCHAR(100),
    email_user                    VARCHAR(255),
    is_active                     BOOLEAN     NOT NULL DEFAULT TRUE,
    last_login_date               TIMESTAMP,
    portal_user_confirmation_date TIMESTAMP,
    created_at                    TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at                    TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    correlation_id                VARCHAR(100),

    CONSTRAINT fk_da_customer FOREIGN KEY (person_id) REFERENCES client.customer (person_id),
    CONSTRAINT uq_da_person UNIQUE (person_id)

);


CREATE INDEX idx_loyalty_status_customer ON client.loyalty_status (identifier_id);
CREATE INDEX idx_loyalty_status_person ON client.loyalty_status (person_id);

CREATE INDEX idx_loyalty_status_code ON client.loyalty_status (status_code);
CREATE INDEX idx_loyalty_status_current ON client.loyalty_status (is_current);

CREATE INDEX idx_language_customer ON client.language (person_id);
CREATE INDEX idx_language_code ON client.language (language_code);

CREATE INDEX idx_nationality_customer ON client.nationality (person_id);
CREATE INDEX idx_nationality_country ON client.nationality (country_code);

CREATE INDEX idx_customer_indicator_person ON client.customer_indicator (person_id);
CREATE INDEX idx_customer_indicator_type ON client.customer_indicator (type);

CREATE INDEX idx_address_customer ON client.address (person_id);
CREATE INDEX idx_address_country ON client.address (country_code);
CREATE INDEX idx_address_current ON client.address (is_current);

CREATE INDEX idx_contact_customer ON client.contact (person_id);
CREATE INDEX idx_contact_type ON client.contact (contact_type);

CREATE INDEX idx_comm_sub_customer ON client.communication_subscription (person_id);
CREATE INDEX idx_comm_sub_code ON client.communication_subscription (communication_code);

CREATE INDEX idx_digital_access_customer ON client.digital_access (person_id);
CREATE INDEX idx_digital_access_username ON client.digital_access (username);
DROP TABLE client.dead_letter;
CREATE TABLE client.dead_letter
(
    id                 SERIAL PRIMARY KEY,
    inserted_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_code         VARCHAR(100),
    error_message      TEXT,
    retry_count        INTEGER                  DEFAULT 0,
    status             VARCHAR(20)              DEFAULT 'NEW'
        CONSTRAINT check_status
            CHECK (status IN ('NEW', 'RETRIED', 'RESOLVED', 'IGNORED')),
    person_id          VARCHAR(50),
    correlation_id     VARCHAR(50),
    source_application VARCHAR(50),
    raw_payload        JSONB NOT NULL
);

ALTER TABLE client.dead_letter
    ADD CONSTRAINT uq_dlq_dedup UNIQUE (person_id, correlation_id, error_code);

CREATE INDEX idx_dlq_person_id ON client.dead_letter (person_id);
CREATE INDEX idx_dlq_correlation_id ON client.dead_letter (correlation_id);
CREATE INDEX idx_dlq_status ON client.dead_letter (status);
CREATE INDEX idx_dlq_inserted_at ON client.dead_letter (inserted_at);
CREATE INDEX idx_dlq_raw_payload ON client.dead_letter USING GIN (raw_payload);

INSERT INTO client.dict_gender (gender_code, gender_name)
VALUES ('M', 'Male'),
       ('F', 'Female');

INSERT INTO client.dict_country (country_code, country_name, number_of_neighbors, access_to_the_sea, population)
VALUES ('PL', 'Poland', 7, TRUE, 37950000),
       ('DE', 'Germany', 9, TRUE, 83200000),
       ('CZ', 'Czech Republic', 4, FALSE, 10830000),
       ('SK', 'Slovakia', 5, FALSE, 5460000),
       ('UA', 'Ukraine', 7, TRUE, 41170000),
       ('LT', 'Lithuania', 4, TRUE, 2870000);

INSERT INTO client.dict_language (language_code, language_name, language_min_level_code, language_max_level_code)
VALUES ('pl', 'Polish', 'A1', 'C2'),
       ('de', 'German', 'A1', 'C2'),
       ('en', 'English', 'A1', 'C2'),
       ('es', 'Spanish', 'A1', 'C2'),
       ('uk', 'Ukrainian', 'A1', 'C2'),
       ('cs', 'Czech', 'A1', 'C2'),
       ('sk', 'Slovak', 'A1', 'C2'),
       ('lt', 'Lithuanian', 'A1', 'C2');

INSERT INTO client.dict_loyalty_status (status_code, status_name, status_rules)
VALUES ('Bronze', 'Bronze', 'Default status upon registration'),
       ('Silver', 'Silver', 'Awarded after 6 months of active membership'),
       ('Gold', 'Gold', 'Awarded after 12 months with regular purchases'),
       ('Platinum', 'Platinum', 'Awarded to top-tier loyal customers');

INSERT INTO client.dict_indicator (indicator_type, indicator_description, indicator_rules)
VALUES ('EMPLOYEE', 'Employee of the retail chain', 'Set manually by HR department'),
       ('SENIOR', 'Senior citizen (60+ years)', 'Auto-assigned when age >= 60, probability 50%'),
       ('KDR', 'Large Family Card holder', 'Auto-assigned when civil_status = married, probability 50%');

INSERT INTO client.dict_civil (civil_status_type, civil_status_description)
VALUES ('single', 'Single / unmarried'),
       ('married', 'Married'),
       ('divorced', 'Divorced'),
       ('widowed', 'Widowed');

INSERT INTO client.dict_contact (contact_type, contact_name, contact_description)
VALUES ('email', 'Email', 'Electronic mail address'),
       ('phone', 'Phone', 'Phone number (mobile or landline)'),
       ('address', 'Address', 'Physical postal address');

INSERT INTO client.dict_subscription (communication_code, communication_name, communication_description)
VALUES ('STORE_PROMO', 'Promotion in local store', 'Notifications about promotions in physical stores'),
       ('ECOMMERCE', 'E-commerce', 'Notifications about online store offers'),
       ('NEWSLETTER', 'Newsletter', 'Periodic newsletter with news and offers'),
       ('LOYALTY_INFO', 'Loyalty information', 'Updates about loyalty program status and benefits');

INSERT INTO client.country_language (country_code, language_code)
VALUES
-- native languages (from COUNTRY_LANGUAGE_MAP)
('PL', 'pl'),
('DE', 'de'),
('CZ', 'cs'),
('SK', 'sk'),
('UA', 'uk'),
('LT', 'lt'),
-- widely spoken second languages
('PL', 'en'),
('DE', 'en'),
('CZ', 'en'),
('SK', 'en'),
('UA', 'en'),
('LT', 'en'),
-- regional/neighbor languages
('CZ', 'sk'),
('SK', 'cs'),
('DE', 'pl'),
('LT', 'pl'),
('UA', 'pl');
