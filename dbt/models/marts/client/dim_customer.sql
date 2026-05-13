{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(person_id)',
) }}

select
    person_id,
    first_name,
    middle_name,
    last_name,
    birth_date,
    passport_number,
    gender_code,
    gender_name,
    civil_status_code,
    civil_status_description,
    language_code,
    language_name,
    language_level,
    nationality_code,
    nationality_name,
    registration_date,
    creation_application,
    is_deleted
from {{ ref('gold_client_dim_customer') }} FINAL
where is_current = 1
