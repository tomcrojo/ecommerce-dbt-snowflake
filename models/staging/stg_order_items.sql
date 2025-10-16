{{
    config(
        materialized='view',
        alias='stg_order_items'
    )
}}

with source as (

    select * from {{ source('raw', 'raw_order_items') }}

),

renamed as (

    select
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price_cents,

        -- Convert to euros
        {{ cents_to_euros('unit_price_cents', 2) }} as unit_price,

        -- Line total: quantity × unit price in euros
        round(quantity * (unit_price_cents / 100.0), 2) as line_total

    from source

)

select * from renamed
