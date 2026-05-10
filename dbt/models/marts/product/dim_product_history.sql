{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(art_key, valid_from)',
) }}

select
    art_key,
    art_number,
    brand,
    article_codification_date,
    department_name,
    sector_code,
    sector_name,
    segment_code,
    segment_name,
    contractor_name,
    is_current,
    dbt_valid_from as valid_from,
    dbt_valid_to   as valid_to
from {{ ref('dim_product_scd') }} FINAL
