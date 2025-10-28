{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',
        alias='fct_orders'
    )
}}

with orders as (

    select * from {{ ref('stg_orders') }}

    {% if is_incremental() %}
    -- Only process orders on or after the last processed order_date
    where order_date >= (select coalesce(max(order_date), '{{ var("start_date") }}') from {{ this }})
    {% endif %}

),

order_items as (

    select * from {{ ref('stg_order_items') }}

),

-- Compute order-level aggregations from line items
order_aggregates as (

    select
        order_id,
        sum(line_total) as total_amount,
        sum(quantity) as item_count,
        count(*) as line_item_count
    from order_items
    group by order_id

),

final as (

    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.order_year,
        o.order_month,
        o.region,
        o.status,
        o.currency,

        -- Financial metrics
        coalesce(agg.total_amount, 0) as total_amount,
        coalesce(agg.item_count, 0) as item_count,
        coalesce(agg.line_item_count, 0) as line_item_count,

        -- Business flags
        case when o.status = 'completed' then true else false end as is_completed,

        -- Audit
        current_timestamp() as _loaded_at

    from orders o

    left join order_aggregates agg
        on o.order_id = agg.order_id

)

select * from final
