{% snapshot gold_site %}

{{ config(
    target_schema='gold_shop',
    unique_key='site_unique_code',
    strategy='check',
    check_cols=[
        'site_code', 'site_name',
        'status_code', 'opening_date', 'closing_date',
        'format_code',
        'zip_code', 'city', 'street', 'city_code', 'country_code', 'latitude', 'longitude'
    ],
) }}

with site_main as (
    select
        site_unique_code,
        code as site_code,
        name as site_name,
        updated_at
    from {{ ref('stg_store__site') }}
),
site_info as (
    select
        site_unique_code,
        status_code,
        opening_date,
        closing_date,
        updated_at
    from {{ ref('stg_store__site_info') }}
    where is_current = 1
),
site_format as (
    select
        site_unique_code,
        format_code
    from {{ ref('stg_store__site_format') }}
    where is_current = 1
),
site_address as (
    select
        site_unique_code,
        zip_code,
        city,
        street,
        city_code,
        country_code,
        latitude,
        longitude
    from {{ ref('stg_store__site_address') }}
    where is_current = 1
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
)

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
    contact.contact_valid_to
from site_main site
left join site_info info        on site.site_unique_code = info.site_unique_code
left join site_format fmt       on site.site_unique_code = fmt.site_unique_code
left join site_address address  on site.site_unique_code = address.site_unique_code
left join site_contact contact  on site.site_unique_code = contact.site_unique_code

{% endsnapshot %}
