{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(dbt_updated_at)',
    order_by='(site_unique_code, dbt_valid_from)',
) }}

with site_main as (
    select
        site_unique_code,
        argMax(code, updated_at) as site_code,
        argMax(name, updated_at) as site_name
    from {{ ref('stg_store__site') }}
    group by site_unique_code
),
site_info as (
    select
        site_unique_code,
        argMax(status_code,   updated_at) as status_code,
        argMax(opening_date,  updated_at) as opening_date,
        argMax(closing_date,  updated_at) as closing_date
    from {{ ref('stg_store__site_info') }}
    group by site_unique_code
),
site_format as (
    select
        site_unique_code,
        argMax(format_code, updated_at) as format_code
    from {{ ref('stg_store__site_format') }}
    group by site_unique_code
),
site_address as (
    select
        site_unique_code,
        argMax(zip_code,      updated_at) as zip_code,
        argMax(city,          updated_at) as city,
        argMax(street,        updated_at) as street,
        argMax(city_code,     updated_at) as city_code,
        argMax(country_code,  updated_at) as country_code,
        argMax(latitude,      updated_at) as latitude,
        argMax(longitude,     updated_at) as longitude
    from {{ ref('stg_store__site_address') }}
    group by site_unique_code
),
site_contact as (
    select
        site_unique_code,
        groupArray(type)                                    as contact_type,
        groupArray(value)                                   as contact_value,
        groupArray(role)                                    as contact_role,
        groupArray(is_primary)                              as contact_is_primary,
        groupArray(valid_from)                              as contact_valid_from,
        groupArray(ifNull(valid_to, toDate('9999-12-31')))  as contact_valid_to
    from {{ ref('stg_store__site_contact') }}
    group by site_unique_code
),
source as (
    select
        site.site_unique_code,
        site.site_code,
        site.site_name,
        info.status_code,
        info.opening_date,
        info.closing_date,
        fmt.format_code,
        address.zip_code,
        address.city,
        address.street,
        address.city_code,
        address.country_code,
        address.latitude,
        address.longitude,
        contact.contact_type,
        contact.contact_value,
        contact.contact_role,
        contact.contact_is_primary,
        contact.contact_valid_from,
        contact.contact_valid_to,
        MD5(concat(
            coalesce(toString(site.site_code),    ''), '|',
            coalesce(toString(site.site_name),    ''), '|',
            coalesce(toString(info.status_code),  ''), '|',
            coalesce(toString(info.opening_date), ''), '|',
            coalesce(toString(info.closing_date), ''), '|',
            coalesce(toString(fmt.format_code),   ''), '|',
            coalesce(toString(address.zip_code),  ''), '|',
            coalesce(toString(address.city),      ''), '|',
            coalesce(toString(address.street),    ''), '|',
            coalesce(toString(address.city_code), ''), '|',
            coalesce(toString(address.country_code), ''), '|',
            coalesce(toString(address.latitude),  ''), '|',
            coalesce(toString(address.longitude), ''), '|',
            coalesce(toString(contact.contact_type), ''), '|',
            coalesce(toString(contact.contact_value), ''), '|',
            coalesce(toString(contact.contact_role), ''), '|',
            coalesce(toString(contact.contact_is_primary), ''), '|',
            coalesce(toString(contact.contact_valid_from), ''), '|',
            coalesce(toString(contact.contact_valid_to), '')
        )) as _row_hash
    from site_main site
    left join site_info info        on site.site_unique_code = info.site_unique_code
    left join site_format fmt       on site.site_unique_code = fmt.site_unique_code
    left join site_address address  on site.site_unique_code = address.site_unique_code
    left join site_contact contact  on site.site_unique_code = contact.site_unique_code
),

{% if is_incremental() %}

current_in_target as (
    select
        site_unique_code,
        argMax(_row_hash,            dbt_valid_from) as _row_hash,
        argMax(site_code,            dbt_valid_from) as site_code,
        argMax(site_name,            dbt_valid_from) as site_name,
        argMax(status_code,          dbt_valid_from) as status_code,
        argMax(opening_date,         dbt_valid_from) as opening_date,
        argMax(closing_date,         dbt_valid_from) as closing_date,
        argMax(format_code,          dbt_valid_from) as format_code,
        argMax(zip_code,             dbt_valid_from) as zip_code,
        argMax(city,                 dbt_valid_from) as city,
        argMax(street,               dbt_valid_from) as street,
        argMax(city_code,            dbt_valid_from) as city_code,
        argMax(country_code,         dbt_valid_from) as country_code,
        argMax(latitude,             dbt_valid_from) as latitude,
        argMax(longitude,            dbt_valid_from) as longitude,
        argMax(contact_type,         dbt_valid_from) as contact_type,
        argMax(contact_value,        dbt_valid_from) as contact_value,
        argMax(contact_role,         dbt_valid_from) as contact_role,
        argMax(contact_is_primary,   dbt_valid_from) as contact_is_primary,
        argMax(contact_valid_from,   dbt_valid_from) as contact_valid_from,
        argMax(contact_valid_to,     dbt_valid_from) as contact_valid_to,
        max(dbt_valid_from)                          as dbt_valid_from
    from {{ this }}
    where dbt_valid_to = toDateTime('9999-12-31 00:00:00')
    group by site_unique_code
),

changed as (
    select s.site_unique_code
    from source s
    inner join current_in_target t on s.site_unique_code = t.site_unique_code
    where s._row_hash != t._row_hash
),

new_sites as (
    select s.site_unique_code
    from source s
    left join current_in_target t on s.site_unique_code = t.site_unique_code
    where t.site_unique_code is null
),

closed_records as (
    select
        t.site_unique_code,
        t.site_code,
        t.site_name,
        t.status_code,
        t.opening_date,
        t.closing_date,
        t.format_code,
        t.zip_code,
        t.city,
        t.street,
        t.city_code,
        t.country_code,
        t.latitude,
        t.longitude,
        t.contact_type,
        t.contact_value,
        t.contact_role,
        t.contact_is_primary,
        t.contact_valid_from,
        t.contact_valid_to,
        t._row_hash,
        0               as is_current,
        t.dbt_valid_from,
        now()           as dbt_valid_to,
        now()           as dbt_updated_at
    from current_in_target t
    inner join changed c on t.site_unique_code = c.site_unique_code
),

new_records as (
    select
        s.site_unique_code,
        s.site_code,
        s.site_name,
        s.status_code,
        s.opening_date,
        s.closing_date,
        s.format_code,
        s.zip_code,
        s.city,
        s.street,
        s.city_code,
        s.country_code,
        s.latitude,
        s.longitude,
        s.contact_type,
        s.contact_value,
        s.contact_role,
        s.contact_is_primary,
        s.contact_valid_from,
        s.contact_valid_to,
        s._row_hash,
        1                                   as is_current,
        now()                               as dbt_valid_from,
        toDateTime('9999-12-31 00:00:00')   as dbt_valid_to,
        now()                               as dbt_updated_at
    from source s
    where s.site_unique_code in (select site_unique_code from changed)
       or s.site_unique_code in (select site_unique_code from new_sites)
)

select * from closed_records
union all
select * from new_records

{% else %}

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
    contact_is_primary,
    contact_valid_from,
    contact_valid_to,
    _row_hash,
    1                                   as is_current,
    now()                               as dbt_valid_from,
    toDateTime('9999-12-31 00:00:00')   as dbt_valid_to,
    now()                               as dbt_updated_at
from source

{% endif %}
