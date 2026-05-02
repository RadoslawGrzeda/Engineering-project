with indicator as (
    select
        person_id,
        indicator,
        is_active,
        updated_at,
        correlation_id
    from {{ ref('stg_client__customer_indicator') }}
),
dict_indicator as (
    select
        indicator_type,
        polish_name,
        english_name
    from {{ ref('stg_client__dict_indicator') }}
)
select
    i.person_id,
    i.indicator as indicator_type,
    di.polish_name,
    di.english_name,
    i.is_active,
    now() as created_at,
    i.updated_at,
    i.correlation_id
from indicator i
left join dict_indicator di 
on i.indicator = di.indicator_type
