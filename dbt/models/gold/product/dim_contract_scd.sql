{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(dbt_updated_at)',
    order_by='(contract_id, dbt_valid_from)',
) }}

with contract as (
    select
        contract_id,
        contractor_id,
        contract_number,
        signed_date
    from {{ ref('stg_product__contract') }}
),
source as (
    select
        contract_id,
        contractor_id,
        contract_number,
        signed_date,
        MD5(concat(
            coalesce(toString(contractor_id),    ''), '|',
            coalesce(toString(contract_number),  ''), '|',
            coalesce(toString(signed_date),      '')
        )) as _row_hash
    from contract
)

{% if is_incremental() %}

, current_in_target as (
    select
        contract_id,
        argMax(_row_hash,       dbt_valid_from) as _row_hash,
        argMax(contractor_id,   dbt_valid_from) as contractor_id,
        argMax(contract_number, dbt_valid_from) as contract_number,
        argMax(signed_date,     dbt_valid_from) as signed_date,
        max(dbt_valid_from)                     as current_dbt_valid_from
    from {{ this }}
    where dbt_valid_to = toDateTime('2106-02-07 06:28:15')
    group by contract_id
),

changed as (
    select s.contract_id
    from source s
    inner join current_in_target t on s.contract_id = t.contract_id
    where s._row_hash != t._row_hash
),

new_entries as (
    select s.contract_id
    from source s
    where s.contract_id not in (select contract_id from current_in_target)
),

closed_records as (
    select
        t.contract_id,
        t.contractor_id,
        t.contract_number,
        t.signed_date,
        t._row_hash,
        0               as is_current,
        t.current_dbt_valid_from as dbt_valid_from,
        now()           as dbt_valid_to,
        now()           as dbt_updated_at
    from current_in_target t
    inner join changed c on t.contract_id = c.contract_id
),

new_records as (
    select
        s.contract_id,
        s.contractor_id,
        s.contract_number,
        s.signed_date,
        s._row_hash,
        1                                   as is_current,
        now()                               as dbt_valid_from,
        toDateTime('2106-02-07 06:28:15')   as dbt_valid_to,
        now()                               as dbt_updated_at
    from source s
    where s.contract_id in (select contract_id from changed)
       or s.contract_id in (select contract_id from new_entries)
)

select * from closed_records
union all
select * from new_records

{% else %}

select
    contract_id,
    contractor_id,
    contract_number,
    signed_date,
    _row_hash,
    1                                   as is_current,
    now()                               as dbt_valid_from,
    toDateTime('2106-02-07 06:28:15')   as dbt_valid_to,
    now()                               as dbt_updated_at
from source

{% endif %}
