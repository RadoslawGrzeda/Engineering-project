with segment_chief as (
    select
        id as segment_chief_id,
        segment_id,
        chief_id
    from {{ ref('stg_product__segment_chief') }}
),
segment as (
    select
        id,
        code as segment_code
    from {{ ref('stg_product__segment') }}
)
select
    sc.segment_chief_id,
    sc.chief_id,
    sc.segment_id,
    s.segment_code,
    1 as is_current,
    now() as valid_from,
    cast(null as Nullable(DateTime)) as valid_to,
    now() as created_at,
    now() as updated_at
from segment_chief sc
left join segment s on sc.segment_id = s.id
