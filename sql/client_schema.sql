CREATE SCHEMA IF NOT EXISTS client;

CREATE TABLE client.dict_gender
(
    gender_code VARCHAR(10) PRIMARY KEY,
    gender_name VARCHAR(50) NOT NULL,
    created_at  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
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
    created_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE client.dict_civil
(
    civil_status_type        VARCHAR(50) PRIMARY KEY,
    civil_status_description VARCHAR(255),
    created_at               TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at               TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE client.dict_contact
(
    contact_type        VARCHAR(50) PRIMARY KEY,
    contact_name        VARCHAR(100) NOT NULL,
    contact_description VARCHAR(255),
    created_at          TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE client.dict_subscription
(
    communication_code        VARCHAR(50) PRIMARY KEY,
    communication_name        VARCHAR(100) NOT NULL,
    communication_description VARCHAR(255),
    created_at                TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at                TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
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

CREATE TABLE client.customer
(
    person_id            VARCHAR(12) PRIMARY KEY,
    first_name           VARCHAR(100) NOT NULL,
    middle_name          VARCHAR(100),
    last_name            VARCHAR(100) NOT NULL,
    birth_date           DATE,
    passport_number      VARCHAR(20),
    gender_code          VARCHAR(10),
    civil_status_code    VARCHAR(50),
    registration_date    TIMESTAMP    NOT NULL,
    creation_application VARCHAR(50),
    is_deleted           BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at           TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    correlation_id       VARCHAR(100),

    CONSTRAINT fk_customer_gender FOREIGN KEY (gender_code) REFERENCES client.dict_gender (gender_code),
    CONSTRAINT fk_customer_civil_status FOREIGN KEY (civil_status_code) REFERENCES client.dict_civil (civil_status_type)

);

CREATE TABLE client.loyalty_status
(
    identifier_id  VARCHAR(12) PRIMARY KEY,
    person_id      VARCHAR(12) NOT NULL,
    status_code    VARCHAR(20) NOT NULL,
    created_at     TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    correlation_id VARCHAR(100),

    CONSTRAINT fk_ls_customer FOREIGN KEY (person_id) REFERENCES client.customer (person_id),
    CONSTRAINT fk_ls_status FOREIGN KEY (status_code) REFERENCES client.dict_loyalty_status (status_code),
    CONSTRAINT uq_loyalty_status_person UNIQUE (person_id)
);

CREATE TABLE client.language
(
    person_id      VARCHAR(12) NOT NULL,
    language_code  VARCHAR(10) NOT NULL,
    language_level VARCHAR(10),
    created_at     TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    correlation_id VARCHAR(100),

    PRIMARY KEY (person_id, language_code),
    CONSTRAINT fk_lang_customer FOREIGN KEY (person_id) REFERENCES client.customer (person_id),
    CONSTRAINT fk_lang_language FOREIGN KEY (language_code) REFERENCES client.dict_language (language_code)
);

CREATE TABLE client.nationality
(
    person_id      VARCHAR(12) NOT NULL,
    country_code   VARCHAR(3)  NOT NULL,
    created_at     TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    correlation_id VARCHAR(100),

    PRIMARY KEY (person_id, country_code),
    CONSTRAINT fk_nat_customer FOREIGN KEY (person_id) REFERENCES client.customer (person_id),
    CONSTRAINT fk_nat_country FOREIGN KEY (country_code) REFERENCES client.dict_country (country_code)
);

CREATE TABLE client.customer_indicator
(
    person_id      VARCHAR(12) NOT NULL,
    type           VARCHAR(50) NOT NULL,
    is_active      BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at     TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    correlation_id VARCHAR(100),

    PRIMARY KEY (person_id, type),
    CONSTRAINT fk_ci_customer FOREIGN KEY (person_id) REFERENCES client.customer (person_id),
    CONSTRAINT fk_ci_indicator FOREIGN KEY (type) REFERENCES client.dict_indicator (indicator_type)

);

CREATE TABLE client.address
(
    person_id        VARCHAR(12) NOT NULL,
    address_type     VARCHAR(50) NOT NULL,
    option_channel   BOOLEAN              DEFAULT TRUE,
    address_street   VARCHAR(200),
    address_zip_code VARCHAR(20),
    address_city     VARCHAR(100),
    country_code     VARCHAR(3),
    geo_coordinates_x_value NUMERIC,
    geo_coordinates_y_value NUMERIC,
    created_at       TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    correlation_id   VARCHAR(100),

    PRIMARY KEY (person_id, address_type),
    CONSTRAINT fk_addr_customer FOREIGN KEY (person_id) REFERENCES client.customer (person_id),
    CONSTRAINT fk_addr_country FOREIGN KEY (country_code) REFERENCES client.dict_country (country_code)
);

CREATE TABLE client.contact
(
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

    PRIMARY KEY (person_id, contact_type),
    CONSTRAINT fk_cont_customer FOREIGN KEY (person_id) REFERENCES client.customer (person_id),
    CONSTRAINT fk_cont_type FOREIGN KEY (contact_type) REFERENCES client.dict_contact (contact_type)
);


CREATE TABLE client.communication_subscription
(
    person_id                VARCHAR(12) NOT NULL,
    communication_code       VARCHAR(50) NOT NULL,
    value                    VARCHAR(255),
    date_of_subscription     TIMESTAMP,
    date_of_unsubscription   TIMESTAMP,
    reason_of_unsubscription VARCHAR(255),
    created_at               TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at               TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    correlation_id           VARCHAR(100),

    PRIMARY KEY (person_id, communication_code),
    CONSTRAINT fk_cs_customer FOREIGN KEY (person_id) REFERENCES client.customer (person_id),
    CONSTRAINT fk_cs_subscription FOREIGN KEY (communication_code) REFERENCES client.dict_subscription (communication_code)
);

CREATE TABLE client.digital_access
(
    person_id                     VARCHAR(12) NOT NULL,
    username                      VARCHAR(100),
    email_user                    VARCHAR(255),
    is_active                     BOOLEAN     NOT NULL DEFAULT TRUE,
    last_login_date               TIMESTAMP,
    portal_user_confirmation_date TIMESTAMP,
    created_at                    TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at                    TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    correlation_id                VARCHAR(100),

    PRIMARY KEY (person_id),
    CONSTRAINT fk_da_customer FOREIGN KEY (person_id) REFERENCES client.customer (person_id)

);

CREATE OR REPLACE FUNCTION client.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_dict_country_updated_at
    BEFORE UPDATE ON client.dict_country
    FOR EACH ROW EXECUTE FUNCTION client.set_updated_at();

CREATE TRIGGER trg_dict_language_updated_at
    BEFORE UPDATE ON client.dict_language
    FOR EACH ROW EXECUTE FUNCTION client.set_updated_at();

CREATE TRIGGER trg_dict_loyalty_status_updated_at
    BEFORE UPDATE ON client.dict_loyalty_status
    FOR EACH ROW EXECUTE FUNCTION client.set_updated_at();

CREATE TRIGGER trg_dict_gender_updated_at
    BEFORE UPDATE ON client.dict_gender
    FOR EACH ROW EXECUTE FUNCTION client.set_updated_at();

CREATE TRIGGER trg_dict_indicator_updated_at
    BEFORE UPDATE ON client.dict_indicator
    FOR EACH ROW EXECUTE FUNCTION client.set_updated_at();

CREATE TRIGGER trg_dict_civil_updated_at
    BEFORE UPDATE ON client.dict_civil
    FOR EACH ROW EXECUTE FUNCTION client.set_updated_at();

CREATE TRIGGER trg_dict_contact_updated_at
    BEFORE UPDATE ON client.dict_contact
    FOR EACH ROW EXECUTE FUNCTION client.set_updated_at();

CREATE TRIGGER trg_dict_subscription_updated_at
    BEFORE UPDATE ON client.dict_subscription
    FOR EACH ROW EXECUTE FUNCTION client.set_updated_at();

CREATE TRIGGER trg_customer_updated_at
    BEFORE UPDATE ON client.customer
    FOR EACH ROW EXECUTE FUNCTION client.set_updated_at();

CREATE TRIGGER trg_loyalty_status_updated_at
    BEFORE UPDATE ON client.loyalty_status
    FOR EACH ROW EXECUTE FUNCTION client.set_updated_at();

CREATE TRIGGER trg_nationality_updated_at
    BEFORE UPDATE ON client.nationality
    FOR EACH ROW EXECUTE FUNCTION client.set_updated_at();

CREATE TRIGGER trg_language_updated_at
    BEFORE UPDATE ON client.language
    FOR EACH ROW EXECUTE FUNCTION client.set_updated_at();

CREATE TRIGGER trg_customer_indicator_updated_at
    BEFORE UPDATE ON client.customer_indicator
    FOR EACH ROW EXECUTE FUNCTION client.set_updated_at();

CREATE TRIGGER trg_address_updated_at
    BEFORE UPDATE ON client.address
    FOR EACH ROW EXECUTE FUNCTION client.set_updated_at();

CREATE TRIGGER trg_contact_updated_at
    BEFORE UPDATE ON client.contact
    FOR EACH ROW EXECUTE FUNCTION client.set_updated_at();

CREATE TRIGGER trg_communication_subscription_updated_at
    BEFORE UPDATE ON client.communication_subscription
    FOR EACH ROW EXECUTE FUNCTION client.set_updated_at();

CREATE TRIGGER trg_digital_access_updated_at
    BEFORE UPDATE ON client.digital_access
    FOR EACH ROW EXECUTE FUNCTION client.set_updated_at();



CREATE INDEX idx_customer_gender ON client.customer (gender_code);
CREATE INDEX idx_customer_civil ON client.customer (civil_status_code);

CREATE INDEX idx_loyalty_status_code ON client.loyalty_status (status_code);
CREATE INDEX idx_loyalty_status_updated ON client.loyalty_status (updated_at);

CREATE INDEX idx_language_code ON client.language (language_code);
CREATE INDEX idx_language_updated ON client.language (updated_at);

CREATE INDEX idx_nationality_country ON client.nationality (country_code);
CREATE INDEX idx_nationality_updated ON client.nationality (updated_at);

CREATE INDEX idx_customer_indicator_type ON client.customer_indicator (type);
CREATE INDEX idx_customer_indicator_updated ON client.customer_indicator (updated_at);

CREATE INDEX idx_address_country ON client.address (country_code);
CREATE INDEX idx_address_updated ON client.address (updated_at);

CREATE INDEX idx_contact_type ON client.contact (contact_type);
CREATE INDEX idx_contact_updated ON client.contact (updated_at);

CREATE INDEX idx_comm_sub_code ON client.communication_subscription (communication_code);
CREATE INDEX idx_comm_sub_updated ON client.communication_subscription (updated_at);

CREATE INDEX idx_digital_access_username ON client.digital_access (username);
CREATE INDEX idx_digital_access_updated ON client.digital_access (updated_at);


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
