with source as (
    select * from
    {{source('bronze', 'product__sector')}} FINAL
)
,renamed as (
    select
        sector_id as id,
        sector_name as name,
        sector_code as code
    from source
    )
select * from renamed