with source_data as (
select * from {{ source('bronze', 'client__customer_indicator') }}
),
renamed as (
select 
    person_id,
    type as indicator,
    is_active = 1 as is_active,
    updated_at,
    correlation_id as correlation_id
from source_data
)
select * from renamed