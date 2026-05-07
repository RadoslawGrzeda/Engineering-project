with source as (
    select * from
    {{source('bronze', 'product__contract')}} FINAL
)

,renamed as (
    select
        contract_id as contract_id,
        contractor_id as contractor_id,
        contract_number as contract_number,
        signed_date as signed_date
    from source
    )
select * from renamed