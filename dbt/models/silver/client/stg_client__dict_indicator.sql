with source_data as (
    select * from {{source('bronze', 'client__dict_indicator') }}
)
,renamed as (
    select
        indicator_type,
        indicator_description as description,
        indicator_rules as rules,
        updated_at
    from source_data
)
select * from renamed