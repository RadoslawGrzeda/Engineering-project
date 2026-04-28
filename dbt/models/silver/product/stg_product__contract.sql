with source as (
    select * from
    {{source('bronze', 'product__contract')}}
)

,renamed as (
    select
        contract_id as contract_id,
        contractor_id as contractor_id,
        contract_number as contract_number,
        signed_date as signed_date,
        case when status = 'active' then 1 else 0 end as status_active,
        is_current as is_current,
        valid_from as valid_from,
        valid_to as valid_to
    from source
    )
select * from renamed