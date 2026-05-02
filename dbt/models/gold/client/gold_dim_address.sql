with address as (
    select
        person_id,
        address_type,
        option_channel,
        street,
        zip_code,
        city,
        country as country_code,
        latitude,
        longitude,
        updated_at,
        correlation_id
    from {{ ref('stg_client__address') }}
),
dict_country as (
    select
        code,
        name
    from {{ ref('stg_client__dict_country') }}
)
select
    a.person_id,
    a.address_type,
    a.option_channel,
    a.street,
    a.zip_code,
    a.city,
    a.country_code,
    dc.name,
    a.latitude,
    a.longitude,
    1 as is_current,
    now() as created_at,
    a.updated_at,
    a.correlation_id
from address a
left join dict_country dc
on a.country_code = dc.code
