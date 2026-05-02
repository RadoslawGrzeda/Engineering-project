with contact as (
    select
        person_id,
        contact_type,
        contact_value,
        main_type,
        preferred_channel,
        option_channel,
        valid,
        updated_at,
        correlation_id
    from {{ ref('stg_client__contact') }}
),
dict_contact as (
    select
        code,
        name
    from {{ ref('stg_client__dict_contact') }}
)
select
    c.person_id,
    c.contact_type,
    dc.name,
    c.contact_value as value,
    c.main_type as flag_main_type,
    c.preferred_channel,
    c.option_channel,
    c.valid as flag_valid,
    1 as is_current,
    now() as created_at,
    c.updated_at,
    c.correlation_id
from contact c
left join dict_contact dc on c.contact_type = dc.code
