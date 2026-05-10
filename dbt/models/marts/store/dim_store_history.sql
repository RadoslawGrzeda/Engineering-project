{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(site_unique_code, valid_from)',
) }}

select
    site_unique_code,
    site_code,
    site_name,
    status_code,
    opening_date,
    closing_date,
    format_code,
    zip_code,
    city,
    street,
    city_code,
    country_code,
    latitude,
    longitude,
    contact_type,
    contact_value,
    contact_role,
    is_current,
    dbt_valid_from as valid_from,
    dbt_valid_to   as valid_to
from {{ ref('dim_store_scd') }} FINAL
