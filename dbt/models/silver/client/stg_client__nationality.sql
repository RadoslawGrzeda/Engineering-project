with source_data as ( 
    select 
        *
    from {{ source('bronze', 'client__nationality') }}
),
renamed as (
    select 
        person_id,
        country_code as code,
        last_ingested_at as updated_at
    from source_data
)

select
    *
from renamed