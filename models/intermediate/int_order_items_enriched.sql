{{
    config(
        materialized='table',
        alias='int_order_items_enriched'
    )
}}

with order_items as (

    select * from {{ ref('stg_order_items') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

products as (

    select * from {{ ref('stg_products') }}

),

enriched as (

    select
        -- Order item identifiers
        oi.order_item_id,
        oi.order_id,
        oi.quantity,
        oi.unit_price_cents,
        oi.unit_price,
        oi.line_total,

        -- Order context
        o.customer_id,
        o.order_date,
        o.order_year,
        o.order_month,
        o.status as order_status,
        o.region,
        o.currency,

        -- Product details
        p.product_id,
        p.product_name,
        p.category as product_category,
        p.subcategory as product_subcategory,
        p.price_tier as product_price_tier,
        p.unit_price as current_catalog_price

    from order_items oi

    inner join orders o
        on oi.order_id = o.order_id

    inner join products p
        on oi.product_id = p.product_id

)

select * from enriched
