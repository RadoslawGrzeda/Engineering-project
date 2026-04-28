with source as (
    select * from
    {{source('bronze', 'product__segment_chief')}}
)

,renamed as (
    select
        segment_chief_id as id,
        segment_id as segment_id,
        chief_id as chief_id
    from source
    )
select * from renamed