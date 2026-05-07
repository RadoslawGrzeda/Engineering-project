with source as (
    select * from
    {{source('bronze', 'product__segment_chief')}} FINAL
)

,renamed as (
    select
        segment_chief_id as id,
        segment_id,
        chief_id,
        valid_from
    from source
    )
select * from renamed