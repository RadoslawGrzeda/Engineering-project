select 
    gender_code,
    gender_name,
    salutation
from {{ source('bronze', 'client__dict_gender') }}


