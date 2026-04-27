select 
    contact_type,
    contact_name,
    contact_description,
    validation_regex,
from {{ source('bronze', 'client__dict_contact') }}
