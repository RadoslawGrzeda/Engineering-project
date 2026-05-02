with all_changes as (
    select
        person_id,
        cityHash64(person_id, updated_at) as correlation_id,
        updated_at as changed_at
    from {{ ref('stg_client__customer') }}
    union all
    select
        person_id,
        cityHash64(person_id, address_type, updated_at) as correlation_id,
        updated_at as changed_at
    from {{ ref('stg_client__address') }}
    union all
    select
        person_id,
        cityHash64(person_id, contact_type, updated_at) as correlation_id,
        updated_at as changed_at
    from {{ ref('stg_client__contact') }}
    union all
    select
        person_id,
        cityHash64(person_id, communication_code, updated_at) as correlation_id,
        updated_at as changed_at
    from {{ ref('stg_client__communication_subscription') }}
    union all
    select
        person_id,
        cityHash64(identifier_id, updated_at) as correlation_id,
        updated_at as changed_at
    from {{ ref('stg_client__loyalty_status') }}
    union all
    select
        person_id,
        cityHash64(person_id, indicator, updated_at) as correlation_id,
        updated_at as changed_at
    from {{ ref('stg_client__customer_indicator') }}
    union all
    select
        person_id,
        cityHash64(person_id, updated_at) as correlation_id,
        updated_at as changed_at
    from {{ ref('stg_client__digital_access') }}
)
select
    cityHash64(person_id, correlation_id, changed_at) as id,
    person_id,
    correlation_id,
    changed_at
from all_changes
