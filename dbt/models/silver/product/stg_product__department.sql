with source as (
    select * from
    {{source('bronze', 'product__department')}}
)

,renamed as (
    select
        department_id as id,
        department_name as name,
        sector_id as sector_id
    from source
    )
select * from renamed

