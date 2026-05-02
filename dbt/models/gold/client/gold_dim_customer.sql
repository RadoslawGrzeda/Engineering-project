with customer as (
    select
        person_id,
        first_name,
        middle_name,
        last_name,
        birth_date,
        passport_number,
        gender_code,
        civil_status_code,
        is_deleted,
        registration_date,
        updated_at,
        correlation_id
    from {{ ref('stg_client__customer') }}
),
digital_access as (
    select
        person_id,
        username,
        email,
        is_active,
        last_login_at,
        portal_user_confirmation_at,
        updated_at,
        correlation_id
    from {{ ref('stg_client__digital_access') }}
),
language_rows as (
    select
        person_id,
        code as language_code,
        level as language_level
    from {{ ref('stg_client__language') }}
),
dict_language as (
    select
        code,
        name as language_name
    from {{ ref('stg_client__dict_language') }}
),
language_enriched as (
    select
        l.person_id,
        l.language_code,
        coalesce(dl.language_name, '') as language_name,
        coalesce(toString(l.language_level), '') as language_level
    from language_rows l
    left join dict_language dl on l.language_code = dl.code
),
languages_by_person as (
    select
        person_id,
        groupArray(
            tuple(
                language_code,
                language_name,
                language_level
            )
        ) as languages
    from language_enriched
    group by person_id
),
nationality_rows as (
    select
        person_id,
        code as nationality_code
    from {{ ref('stg_client__nationality') }}
),
dict_country as (
    select
        code,
        name
    from {{ ref('stg_client__dict_country') }}
),
nationality_enriched as (
    select
        n.person_id,
        n.nationality_code,
        coalesce(dc.name, '') as country_name
    from nationality_rows n
    left join dict_country dc on n.nationality_code = dc.code
),
nationalities_by_person as (
    select
        person_id,
        groupArray(tuple(nationality_code, country_name)) as nationalities
    from nationality_enriched
    group by person_id
),
dict_gender as (
    select
        code,
        name,
        salutation
    from {{ ref('stg_client__dict_gender') }}
),
dict_civil as (
    select
        code,
        polish_display_name as civil_status_name,
        is_partnership
    from {{ ref('stg_client__dict_civil') }}
)
select
    c.person_id,
    c.first_name,
    c.middle_name,
    c.last_name,
    c.birth_date,
    c.passport_number,
    c.gender_code,
    g.name as gender_name,
    g.salutation,
    c.civil_status_code,
    cv.civil_status_name,
    cv.is_partnership,
    ifNull(lp.languages, cast([] as Array(Tuple(String, String, String)))) as languages,
    ifNull(np.nationalities, cast([] as Array(Tuple(String, String)))) as nationalities,
    da.username,
    da.email,
    da.is_active,
    da.last_login_at,
    da.portal_user_confirmation_at,
    c.registration_date,
    c.is_deleted,
    now() as created_at,
    c.correlation_id,
    c.updated_at
from customer c
left join digital_access da on c.person_id = da.person_id
left join languages_by_person lp on c.person_id = lp.person_id
left join nationalities_by_person np on c.person_id = np.person_id
left join dict_gender g on c.gender_code = g.code
left join dict_civil cv on c.civil_status_code = cv.code
