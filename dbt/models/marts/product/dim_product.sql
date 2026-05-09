{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(art_key)',
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
    contractor_name
from {{ ref('dim_product') }} FINAL
where is_current = 1
