with source as (
    select * from
    {{source('bronze', 'product__segment_chief')}}
)

,renamed as (
    select
        segment_chief_id as id,
        segment_id,
        chief_id,
        is_current,
        valid_from,
        valid_to
    from source
    )
select * from renamed