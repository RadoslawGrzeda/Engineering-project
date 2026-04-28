with product as (
    select 
    id,
    art_number,
    segment_id,
    contractor_id,
    department_id,
    brand,
    article_codification_date
    from {{ source('silver', 'stg_product__product') }}
),
departament as (
    select
        id,
        name,
        sector_id
    from {{ ref('stg_product__department') }}
    

),
,sector as (
    select
        id,
        name,
        code
    from {{ ref('stg_product__sector') }}
    )
,contractor as (
    select
        id,
        name,
        -- phone_number,
        -- email,
        -- address
    from {{ ref('stg_product__contractor') }}
    )
,segment as (
    select
        id,
        code,
        name,
        sector_id
    from {{ ref('stg_product__segment') }}
    )

select 
    product.id,
    product.art_number,
    product.contractor_id,
    product.segment_id,
    product.brand,
    product.article_codification_date
    product.department_id,
    departament.name as department_name,
    sector.id as sector_id
    sector.name as sector_name,
    segment.code as segment_code,
    segment.name as segment_name,
    contractor.name as contractor_name,
    1 as is_current
    now() as created_at,
    now() as updated_at
from product 
join departament on product.department_id = departament.id
join sector on departament.sector_id = sector.id
join contractor on product.contractor_id = contractor.id
join segment on product.segment_id = segment.id