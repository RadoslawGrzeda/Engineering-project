with source_data as (
    select * from {{ source('bronze', 'client__dict_gender') }}
)
,renamed as (
    select 
        gender_code as code,
        gender_name as name,
        salutation as salutation
    from source_data
)
select * from renamed


