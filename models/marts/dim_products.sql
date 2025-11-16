{{
    config(
        materialized='table',
        alias='dim_products'
    )
}}

with products as (

    select * from {{ ref('stg_products') }}

),

-- Determine if product has been ordered (active in catalog)
product_orders as (

    select
        product_id,
        count(*) as times_ordered,
        sum(quantity) as total_units_sold,
        max(order_date) as last_order_date
    from {{ ref('int_order_items_enriched') }}
    group by product_id

),

final as (

    select
        p.product_id,
        p.product_name,
        p.category,
        p.subcategory,
        p.price_tier,
        p.unit_price_cents,
        p.unit_price,

        -- Order activity metrics
        coalesce(po.times_ordered, 0) as times_ordered,
        coalesce(po.total_units_sold, 0) as total_units_sold,
        po.last_order_date,

        -- Active flag: ordered at least once
        case when po.product_id is not null then true else false end as is_active,

        current_timestamp as _loaded_at

    from products p

    left join product_orders po
        on p.product_id = po.product_id

)

select * from final
