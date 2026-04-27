with source_data as (
    select *
    from {{ source('bronze', 'client__dict_loyalty_status') }}
)
,renamed as (
    select 
        status_code as code,
        status_name as name,
        status_rules  as rules
    from source_data
)
select * from renamed