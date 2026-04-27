with source_data as (
select 
    *
from {{ source('bronze', 'client__dict_language') }}
)
,renamed as (
    select 
    language_code as code,
    language_name as name,
    language_min_level_code as min_level_code,
    language_max_level_code as max_level_code
    from source_data
)
select * from renamed