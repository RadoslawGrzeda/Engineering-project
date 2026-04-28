with source_data as (
    select *
    from {{ source('bronze', 'client__dict_subscription') }}
)
,renamed as (
    select 
        communication_code as code,
        communication_name as name,
        communication_description as description
    from source_data
)
select * from renamed