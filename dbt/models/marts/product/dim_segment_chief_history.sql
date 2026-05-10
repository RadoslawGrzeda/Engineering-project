{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(segment_chief_id, valid_from)',
) }}

select
    segment_chief_id,
    chief_id,
    segment_id,
    segment_code,
    src_valid_from,
    is_current,
    dbt_valid_from as valid_from,
    dbt_valid_to   as valid_to
from {{ ref('dim_segment_chief_scd') }} FINAL
