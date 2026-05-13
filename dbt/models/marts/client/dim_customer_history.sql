{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(person_id, valid_from)',
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
    is_deleted,
    is_current,
    dbt_valid_from as valid_from,
    dbt_valid_to   as valid_to
from {{ ref('gold_client_dim_customer') }} FINAL
