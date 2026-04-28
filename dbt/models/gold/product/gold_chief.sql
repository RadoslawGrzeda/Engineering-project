with source as (
    select 
    id,
    first_name,
    last_name,
    phone_number,
    address,
    from 
    {{ source('silver', 'stg_chief') }}
)
select 
    id,
    first_name,
    last_name,
    phone_number,
    address
from source