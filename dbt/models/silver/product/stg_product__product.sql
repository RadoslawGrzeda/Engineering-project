with source as (
    select * from
    {{source('bronze', 'product__product')}} FINAL
)
,renamed as (
    select
        art_key                   as id,
        art_number,
        segment_id,
        department_id,
        contractor_id,
        brand,
        article_codification_date,
        updated_at
    from source
    )
select * from renamed