select
    country_code,
    country_name,
    number_of_neighbors,
    access_to_the_sea = 1 as sea_access,
    population
from {{ source('bronze', 'client__dict_country') }}
