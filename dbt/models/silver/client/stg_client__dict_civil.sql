with source_data as (
    select * from {{ source('bronze', 'client__dict_civil') }}
)
,renamed as (
    select 
        civil_status_type as type,
        civil_status_code,
        is_partnership = 1 as is_partnership,
        display_name_pl as polish_display_name,
        display_name_en as english_display_name
    from source_data
)
select * from renamed