with source as (
    select * from
    {{source('bronze', 'product__segment')}} FINAL
)

,renamed as (
    select
        segment_id as id,
        segment_code as code,
        segment_name as name,
        sector_id as sector_id
    from source
    )
select * from renamed