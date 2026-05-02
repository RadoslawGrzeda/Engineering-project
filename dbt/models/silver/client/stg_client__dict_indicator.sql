with source_data as (
    select * from {{source('bronze', 'client__dict_indicator') }}
)
,renamed as (
    select 
        indicator_type,
        display_name_pl as polish_name,
        display_name_en as english_name,
        description_pl as polish_description,
        description_en as english_description,
        calc_rules_desc as calculation_rules,
        is_automated = 1 as is_automated,
        is_active = 1 as is_active
    from source_data
)
select * from renamed