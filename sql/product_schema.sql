CREATE SCHEMA IF NOT EXISTS product;

create table if not exists product.sector (
        sector_id int,
        sector_name varchar(255) not null,
        sector_code varchar(255) not null,
        created_at timestamp default CURRENT_TIMESTAMP,
        updated_at timestamp default CURRENT_TIMESTAMP,
        source_file varchar(255) not null,
        primary key (sector_id)
);

create table if not exists product.department(
    department_id int,
    department_name varchar(255) not null,
    sector_id int not null,
    created_at timestamp default CURRENT_TIMESTAMP,
    updated_at timestamp default CURRENT_TIMESTAMP,
    source_file varchar(255) not null,
    primary key (department_id),
    foreign key (sector_id) references product.sector(sector_id)
);

create table if not exists product.segment(
    segment_id int,
    segment_code varchar(255) not null,
    segment_name varchar(255) not null,
    sector_id int not null,
    created_at timestamp default CURRENT_TIMESTAMP,
    updated_at timestamp default CURRENT_TIMESTAMP,
    source_file varchar(255) not null,
    primary key (segment_id),
    foreign key (sector_id) references product.sector(sector_id)
);

create table if not exists product.chief
(
    chief_id         varchar,
    chief_first_name varchar(255) not null,
    chief_last_name  varchar(255) not null,
    phone_number     varchar(255) not null,
    email_address    varchar(255) not null,
    created_at       timestamp default CURRENT_TIMESTAMP,
    updated_at       timestamp default CURRENT_TIMESTAMP,
    source_file      varchar(255) not null,
    primary key (chief_id)
);

create table if not exists product.segment_chief (
    segment_chief_id serial,
    segment_id int,
    chief_id varchar,
    valid_from date default CURRENT_DATE,
    created_at timestamp default CURRENT_TIMESTAMP,
    updated_at timestamp default CURRENT_TIMESTAMP,
    source_file varchar(255) not null,
    primary key (segment_chief_id),
    unique (segment_id),
    foreign key (segment_id) references product.segment(segment_id),
    foreign key (chief_id) references product.chief(chief_id)
);

create table if not exists product.contractor (
    contractor_id int primary key,
    contractor_name varchar(255) not null,
    contractor_phone_number varchar(255) not null,
    contractor_email_address varchar(255) not null,
    contractor_address varchar(255) not null,
    created_at timestamp default CURRENT_TIMESTAMP,
    updated_at timestamp default CURRENT_TIMESTAMP,
    source_file varchar(255) not null
);

create table if not exists product.contract
(
    contract_id     serial primary key,
    contractor_id   int                 not null,
    contract_number varchar(255) unique not null,
    signed_date     date                not null,
    status          varchar(255)        not null,
    created_at      timestamp default CURRENT_TIMESTAMP,
    updated_at      timestamp default CURRENT_TIMESTAMP,
    source_file     varchar(255)        not null,
    foreign key (contractor_id) references product.contractor(contractor_id),
    unique (contractor_id, contract_number)
);

create table if not exists product.product (
    art_key int primary key,
    art_number varchar(255) not null,
    contractor_id int not null,
    segment_id int not null,
    department_id int not null,
    brand varchar(255) not null,
    article_codification_date date default CURRENT_DATE,
    created_at timestamp default CURRENT_TIMESTAMP,
    updated_at timestamp default CURRENT_TIMESTAMP,
    source_file varchar(255) not null,
    foreign key (contractor_id) references product.contractor(contractor_id),
    foreign key (segment_id) references product.segment(segment_id),
    foreign key (department_id) references product.department(department_id)
);

create table if not exists product.pos_information
(
    pos_information_id serial,
    art_key            int            not null,
    ean                varchar(13)    not null unique,
    vat_rate           decimal(5, 2)  not null,
    price_net          decimal(10, 2) not null,
    price_gross        decimal(10, 2) not null,
    valid_from         date           not null,
    created_at         timestamp default CURRENT_TIMESTAMP,
    updated_at         timestamp default CURRENT_TIMESTAMP,
    source_file        varchar(255)   not null,
    primary key (pos_information_id),
    foreign key (art_key) references product.product(art_key),
    unique (art_key, ean)
);
CREATE INDEX idx_chief_phone_number  ON product.chief(phone_number);
CREATE INDEX idx_chief_email_address ON product.chief(email_address);
CREATE INDEX idx_chief_last_name     ON product.chief(chief_last_name);

CREATE INDEX idx_contract_contractor_id    ON product.contract(contractor_id);

CREATE INDEX idx_contractor_email_address ON product.contractor(contractor_email_address);


CREATE INDEX idx_department_sector_id      ON product.department(sector_id);

CREATE INDEX idx_segment_sector_id         ON product.segment(sector_id);

CREATE INDEX idx_segment_chief_chief_id    ON product.segment_chief(chief_id);


CREATE INDEX idx_product_contractor_id     ON product.product(contractor_id);
CREATE INDEX idx_product_segment_id        ON product.product(segment_id);
CREATE INDEX idx_product_department_id     ON product.product(department_id);

CREATE INDEX idx_pos_information_price_gross ON product.pos_information(price_gross);
CREATE INDEX idx_pos_information_valid_from  ON product.pos_information(valid_from);

CREATE OR REPLACE FUNCTION product.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_sector_updated_at
    BEFORE UPDATE ON product.sector
    FOR EACH ROW EXECUTE FUNCTION product.set_updated_at();

CREATE TRIGGER trg_department_updated_at
    BEFORE UPDATE ON product.department
    FOR EACH ROW EXECUTE FUNCTION product.set_updated_at();

CREATE TRIGGER trg_segment_updated_at
    BEFORE UPDATE ON product.segment
    FOR EACH ROW EXECUTE FUNCTION product.set_updated_at();

CREATE TRIGGER trg_chief_updated_at
    BEFORE UPDATE ON product.chief
    FOR EACH ROW EXECUTE FUNCTION product.set_updated_at();

CREATE TRIGGER trg_segment_chief_updated_at
    BEFORE UPDATE ON product.segment_chief
    FOR EACH ROW EXECUTE FUNCTION product.set_updated_at();

CREATE TRIGGER trg_contractor_updated_at
    BEFORE UPDATE ON product.contractor
    FOR EACH ROW EXECUTE FUNCTION product.set_updated_at();

CREATE TRIGGER trg_contract_updated_at
    BEFORE UPDATE ON product.contract
    FOR EACH ROW EXECUTE FUNCTION product.set_updated_at();

CREATE TRIGGER trg_product_updated_at
    BEFORE UPDATE ON product.product
    FOR EACH ROW EXECUTE FUNCTION product.set_updated_at();

CREATE TRIGGER trg_pos_information_updated_at
    BEFORE UPDATE ON product.pos_information
    FOR EACH ROW EXECUTE FUNCTION product.set_updated_at();


