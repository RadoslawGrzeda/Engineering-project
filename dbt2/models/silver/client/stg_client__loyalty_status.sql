with source_data as (
select *
from {{ source('bronze', 'client__loyalty_status') }}
)
,renamed as (
select
    identifier_id,  
    person_id,
    status_code as status,
    start_date as start_at,
    end_date as end_at,
    evaluation_date as evaluation_at,
    last_ingested_at as updated_at
from source_data
)
select * from renamed
