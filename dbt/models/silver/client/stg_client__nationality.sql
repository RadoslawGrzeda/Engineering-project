with source_data as ( 
    select 
        *
    from {{ source('bronze', 'client__nationality') }}
),

renamed as (
    select 
        person_id,
        country_code as code,
        updated_at,
        correlation_id,
        correlation_id
    from source_data
)

select
    *
from renamed