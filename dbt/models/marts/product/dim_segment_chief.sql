{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(segment_chief_id)',
) }}

select
    segment_chief_id,
    chief_id,
    segment_id,
    segment_code,
    src_valid_from
from {{ ref('dim_segment_chief') }} FINAL
where is_current = 1
