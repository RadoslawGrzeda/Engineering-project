with chief as (
    select
        id as chief_id,
        first_name,
        last_name,
        phone_number,
        email
    from {{ ref('stg_product__chief') }}
)
select
    chief_id,
    first_name,
    last_name,
    phone_number,
    email,
    1 as is_active,
    now() as created_at,
    now() as updated_at
from chief
