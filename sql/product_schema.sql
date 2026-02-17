-- drop table sector

-- select * from sector

create  table sector (
        sector_id int,
        sector_name varchar(255) not null,
        sector_code varchar(255) not null,
        created_at timestamp default current_timestamp,
        source_file varchar(255) not null,
        primary key (sector_id)
);

create table department(
    department_id int,
    department_name varchar(255) not null,
    sector_id int not null,
    created_at timestamp default current_timestamp,
    source_file varchar(255) not null,
    primary key (department_id),
    foreign key (sector_id) references sector(sector_id)
);

create table segment(
    segment_id int,
    segment_code varchar(255) not null,
    segment_name varchar(255) not null,
    sector_id int not null,
    created_at timestamp default current_timestamp,
    source_file varchar(255) not null,
    primary key (segment_id),
    foreign key (sector_id) references sector(sector_id)
);


-- insert_relation_sql = text(f'''
--                                 INSERT INTO segment_chief (segment_id, chief_id, is_current, valid_from, valid_to, source_file)
--                                 VALUES (:segment_id, :chief_id, :is_current, :date_start, :date_end, :source_file)
--                                 ON CONFLICT (segment_id, chief_id) DO UPDATE SET
--                                 is_current = True,
--                                 valid_from = current_date(),
--                                 valid_to = NULL,
--                                 source_file = EXCLUDED.source_file;
--
--                                 ''')


create table segment_chief (
    segment_id int,
    chief_id varchar,
    is_current boolean default true,
    valid_from date default current_date,
    valid_to date,
    source_file varchar(255) not null,
    primary key (segment_id, chief_id),
    foreign key (segment_id) references segment(segment_id),
    foreign key (chief_id) references chief(chief_id)
);

create table chief
(
    chief_id         varchar,
    chief_first_name varchar(255) not null,
    chief_last_name  varchar(255) not null,
    phone_number     varchar(255) not null,
    email_address    varchar(255) not null,
    created_at       timestamp default current_timestamp,
    updated_at       timestamp default current_timestamp,
    source_file      varchar(255) not null,
    primary key (chief_id)
);


create table pos_information
(
    pos_information_id serial,
    art_key            int            not null,
    ean                varchar(13)    not null unique,
    vat_rate           decimal(5, 2)  not null,
    price_net          decimal(10, 2) not null,
    price_gross        decimal(10, 2) not null,
    date_start         date           not null,
    date_end           date,
    is_current         boolean   default true,
    last_modified_date timestamp default current_timestamp,
    source_file        varchar(255)   not null,
    primary key (pos_information_id),
    foreign key (art_key) references product (art_key),
    unique (art_key, ean)
);
-- ALTER TABLE pos_information ADD CONSTRAINT art_ean UNIQUE (art_key, ean);
            --
--         select * from pos_information where art_key in(100001,100003,100003)
--
--
-- select * from segment
-- select * from segment_chief
-- select * from department
-- INSERT INTO product ( art_key, art_number, contractor_id, segment_id, department_id, brand, article_codification_date, last_modified_at, source_file)
--
create table product (
    product_id serial,
    art_key int,
    art_number varchar(255) not null,
    contractor_id int not null,
    segment_id int not null,
    department_id int not null,
    brand varchar(255) not null,
    article_codification_date date default current_date,
    last_modified_at timestamp default current_timestamp,
    source_file varchar(255) not null,
    primary key (product_id),
    foreign key (contractor_id) references contractor(contractor_id),
    foreign key (segment_id) references segment(segment_id),
    foreign key (department_id) references department(department_id)
);

-- select * from product

create table contractor (
    contractor_id int primary key,
    contractor_name varchar(255) not null,
    contractor_phone_number varchar(255) not null,
    contractor_email_address varchar(255) not null,
    contractor_address varchar(255) not null,
    created_at timestamp default current_timestamp,
    updated_at timestamp default current_timestamp,
    source_file varchar(255) not null
    
);

-- drop table contractor cascade
create table contract
(
    contract_id     serial primary key,
    contractor_id   int                 not null,
    contract_number varchar(255) unique not null,
    signed_date     date                not null,
    status          varchar(255)        not null,
    is_current      boolean   default true,
    valid_from      timestamp default current_timestamp,
    valid_to        timestamp,
    source_file     varchar(255)        not null,
    foreign key (contractor_id) references contractor (contractor_id),
    unique (contractor_id, contract_number)
);
