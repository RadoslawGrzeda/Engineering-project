with source_data as (
select
 *
from
{{ source('bronze', 'client__customer_indicator') }}
),
renamed as (
select
    person_id,
    type as indicator,
    is_active = 1 as is_active,
    last_ingested_at as updated_at
from source_data
)
select * from renamed