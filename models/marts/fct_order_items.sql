{{
    config(
        materialized='table',
        alias='fct_order_items'
    )
}}

with enriched as (

    select * from {{ ref('int_order_items_enriched') }}

),

final as (

    select
        order_item_id,
        order_id,
        customer_id,
        product_id,
        product_name,
        product_category,
        product_subcategory,
        product_price_tier,
        quantity,
        unit_price_cents,
        unit_price,
        line_total,
        order_date,
        order_year,
        order_month,
        order_status,
        region,
        currency,

        -- Flag for completed order items (revenue-recognized)
        case when order_status = 'completed' then true else false end as is_revenue_recognized,

        current_timestamp() as _loaded_at

    from enriched

)

select * from final
