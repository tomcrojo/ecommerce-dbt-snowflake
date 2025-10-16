{{
    config(
        materialized='view',
        alias='stg_products'
    )
}}

with source as (

    select * from {{ source('raw', 'raw_products') }}

),

renamed as (

    select
        product_id,
        trim(product_name) as product_name,
        trim(category) as category,
        trim(subcategory) as subcategory,
        unit_price_cents,

        -- Convert to euros for downstream consumption
        {{ cents_to_euros('unit_price_cents', 2) }} as unit_price,

        -- Price tier classification for business analysis
        case
            when unit_price_cents < 1500 then 'budget'        -- < €15
            when unit_price_cents < 15000 then 'standard'     -- < €150
            else 'premium'                                     -- >= €150
        end as price_tier

    from source

)

select * from renamed
