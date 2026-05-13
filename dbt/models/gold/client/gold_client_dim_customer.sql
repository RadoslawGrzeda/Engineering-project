{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(dbt_updated_at)',
    order_by='(person_id, dbt_valid_from)',
) }}

with customer as (
    select
        person_id,
        argMax(first_name,              updated_at) as first_name,
        argMax(middle_name,             updated_at) as middle_name,
        argMax(last_name,               updated_at) as last_name,
        argMax(birth_date,              updated_at) as birth_date,
        argMax(passport_number,         updated_at) as passport_number,
        argMax(gender_code,             updated_at) as gender_code,
        argMax(civil_status_code,       updated_at) as civil_status_code,
        argMax(is_deleted,              updated_at) as is_deleted,
        argMax(registration_date,       updated_at) as registration_date,
        argMax(creation_application,    updated_at) as creation_application
    from {{ ref('stg_client__customer') }}
    group by person_id
),
languages_by_person as (
    select
        l.person_id,
        arrayStringConcat(arraySort(groupArray(l.code)), ',')                           as language_code,
        arrayStringConcat(arraySort(groupArray(coalesce(dl.name, ''))), ',')             as language_name,
        arrayStringConcat(arraySort(groupArray(coalesce(toString(l.level), ''))), ',')  as language_level
    from {{ ref('stg_client__language') }} l
    left join {{ ref('stg_client__dict_language') }} dl on l.code = dl.code
    group by l.person_id
),
nationalities_by_person as (
    select
        n.person_id,
        arrayStringConcat(arraySort(groupArray(n.code)), ',')                   as nationality_code,
        arrayStringConcat(arraySort(groupArray(coalesce(dc.name, ''))), ',')    as nationality_name
    from {{ ref('stg_client__nationality') }} n
    left join {{ ref('stg_client__dict_country') }} dc on n.code = dc.code
    group by n.person_id
),
dict_gender as (
    select code, name
    from {{ ref('stg_client__dict_gender') }}
),
dict_civil as (
    select type, description as civil_status_description
    from {{ ref('stg_client__dict_civil') }}
),
source as (
    select
        cust.person_id                                              as person_id,
        cust.first_name                                             as first_name,
        cust.middle_name                                            as middle_name,
        cust.last_name                                              as last_name,
        cust.birth_date                                             as birth_date,
        cust.passport_number                                        as passport_number,
        cust.gender_code                                            as gender_code,
        g.name                                                      as gender_name,
        cust.civil_status_code                                      as civil_status_code,
        if(cv.type = '', NULL, cv.civil_status_description)         as civil_status_description,
        coalesce(lp.language_code,      '')                         as language_code,
        coalesce(lp.language_name,      '')                         as language_name,
        coalesce(lp.language_level,     '')                         as language_level,
        coalesce(np.nationality_code,   '')                         as nationality_code,
        coalesce(np.nationality_name,   '')                         as nationality_name,
        cust.registration_date                                      as registration_date,
        cust.creation_application                                   as creation_application,
        cust.is_deleted                                             as is_deleted,
        MD5(concat(
            coalesce(toString(cust.first_name),          ''), '|',
            coalesce(toString(cust.middle_name),         ''), '|',
            coalesce(toString(cust.last_name),           ''), '|',
            coalesce(toString(cust.birth_date),          ''), '|',
            coalesce(toString(cust.passport_number),     ''), '|',
            coalesce(toString(cust.gender_code),         ''), '|',
            coalesce(toString(cust.civil_status_code),   ''), '|',
            coalesce(toString(cust.is_deleted),          ''), '|',
            coalesce(toString(cust.registration_date),   ''), '|',
            coalesce(lp.language_code,                   ''), '|',
            coalesce(lp.language_level,                  ''), '|',
            coalesce(np.nationality_code,                '')
        )) as _row_hash
    from customer cust
    left join languages_by_person lp     on cust.person_id = lp.person_id
    left join nationalities_by_person np on cust.person_id = np.person_id
    left join dict_gender g              on cust.gender_code = g.code
    left join dict_civil cv              on cust.civil_status_code = cv.type
)

{% if is_incremental() %}

, current_in_target as (
    select
        person_id,
        argMax(_row_hash,               dbt_valid_from)  as _row_hash,
        argMax(first_name,              dbt_valid_from)  as first_name,
        argMax(middle_name,             dbt_valid_from)  as middle_name,
        argMax(last_name,               dbt_valid_from)  as last_name,
        argMax(birth_date,              dbt_valid_from)  as birth_date,
        argMax(passport_number,         dbt_valid_from)  as passport_number,
        argMax(gender_code,             dbt_valid_from)  as gender_code,
        argMax(gender_name,             dbt_valid_from)  as gender_name,
        argMax(civil_status_code,       dbt_valid_from)  as civil_status_code,
        argMax(civil_status_description, dbt_valid_from) as civil_status_description,
        argMax(language_code,           dbt_valid_from)  as language_code,
        argMax(language_name,           dbt_valid_from)  as language_name,
        argMax(language_level,          dbt_valid_from)  as language_level,
        argMax(nationality_code,        dbt_valid_from)  as nationality_code,
        argMax(nationality_name,        dbt_valid_from)  as nationality_name,
        argMax(registration_date,       dbt_valid_from)  as registration_date,
        argMax(creation_application,    dbt_valid_from)  as creation_application,
        argMax(is_deleted,              dbt_valid_from)  as is_deleted,
        max(dbt_valid_from)                              as current_dbt_valid_from
    from {{ this }}
    where dbt_valid_to = toDateTime('2106-02-07 06:28:15')
    group by person_id
),

changed as (
    select s.person_id
    from source s
    inner join current_in_target t on s.person_id = t.person_id
    where s._row_hash != t._row_hash
),

new_entries as (
    select sc.person_id
    from source sc
    where sc.person_id not in (select person_id from current_in_target)
),

closed_records as (
    select
        t.person_id,
        t.first_name,
        t.middle_name,
        t.last_name,
        t.birth_date,
        t.passport_number,
        t.gender_code,
        t.gender_name,
        t.civil_status_code,
        t.civil_status_description,
        t.language_code,
        t.language_name,
        t.language_level,
        t.nationality_code,
        t.nationality_name,
        t.registration_date,
        t.creation_application,
        t.is_deleted,
        t._row_hash,
        0               as is_current,
        t.current_dbt_valid_from as dbt_valid_from,
        now()           as dbt_valid_to,
        now()           as dbt_updated_at
    from current_in_target t
    inner join changed c on t.person_id = c.person_id
),

new_records as (
    select
        s.person_id,
        s.first_name,
        s.middle_name,
        s.last_name,
        s.birth_date,
        s.passport_number,
        s.gender_code,
        s.gender_name,
        s.civil_status_code,
        s.civil_status_description,
        s.language_code,
        s.language_name,
        s.language_level,
        s.nationality_code,
        s.nationality_name,
        s.registration_date,
        s.creation_application,
        s.is_deleted,
        s._row_hash,
        1                                   as is_current,
        now()                               as dbt_valid_from,
        toDateTime('2106-02-07 06:28:15')   as dbt_valid_to,
        now()                               as dbt_updated_at
    from source s
    where s.person_id in (select person_id from changed)
       or s.person_id in (select person_id from new_entries)
)

select * from closed_records
union all
select * from new_records

{% else %}

select
    cust.person_id,
    cust.first_name,
    cust.middle_name,
    cust.last_name,
    cust.birth_date,
    cust.passport_number,
    cust.gender_code,
    g.name                                                  as gender_name,
    cust.civil_status_code,
    if(cv.type = '', NULL, cv.civil_status_description)     as civil_status_description,
    coalesce(lp.language_code,      '')                     as language_code,
    coalesce(lp.language_name,      '')                     as language_name,
    coalesce(lp.language_level,     '')                     as language_level,
    coalesce(np.nationality_code,   '')                     as nationality_code,
    coalesce(np.nationality_name,   '')                     as nationality_name,
    cust.registration_date,
    cust.creation_application,
    cust.is_deleted,
    MD5(concat(
        coalesce(toString(cust.first_name),          ''), '|',
        coalesce(toString(cust.middle_name),         ''), '|',
        coalesce(toString(cust.last_name),           ''), '|',
        coalesce(toString(cust.birth_date),          ''), '|',
        coalesce(toString(cust.passport_number),     ''), '|',
        coalesce(toString(cust.gender_code),         ''), '|',
        coalesce(toString(cust.civil_status_code),   ''), '|',
        coalesce(toString(cust.is_deleted),          ''), '|',
        coalesce(toString(cust.registration_date),   ''), '|',
        coalesce(lp.language_code,                   ''), '|',
        coalesce(lp.language_level,                  ''), '|',
        coalesce(np.nationality_code,                '')
    )) as _row_hash,
    1                                   as is_current,
    now()                               as dbt_valid_from,
    toDateTime('2106-02-07 06:28:15')   as dbt_valid_to,
    now()                               as dbt_updated_at
from customer cust
left join languages_by_person lp     on cust.person_id = lp.person_id
left join nationalities_by_person np on cust.person_id = np.person_id
left join dict_gender g              on cust.gender_code = g.code
left join dict_civil cv              on cust.civil_status_code = cv.type

{% endif %}
