with source as (
    select * from
    {{source('bronze', 'product__product')}}
)

,renamed as (
    select
        art_key as id,
        art_number as art_number,
        segment_id as segment_id,
        department_id as department_id,
        contractor_id as contractor_id,
        brand as brand,
        article_codification_date as article_codification_date
    from source
    )
select * from renamed