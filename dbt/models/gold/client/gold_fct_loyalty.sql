with loyalty as (
    select
        identifier_id,
        person_id,
        status as status_code,
        start_at,
        end_at,
        evaluation_at,
        updated_at,
        correlation_id
    from {{ ref('stg_client__loyalty_status') }}
),
dict_loyalty as (
    select
        code,
        name as status_name,
        rules as status_rules
    from {{ ref('stg_client__dict_loyalty_status') }}
)
select
    l.identifier_id,
    l.person_id,
    l.status_code,
    dl.status_name,
    dl.status_rules,
    l.start_at,
    l.end_at,
    l.evaluation_at,
    now() as created_at,
    l.correlation_id,
    l.updated_at
from loyalty l
left join dict_loyalty dl on l.status_code = dl.code
