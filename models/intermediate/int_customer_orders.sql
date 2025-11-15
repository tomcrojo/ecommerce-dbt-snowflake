{{
    config(
        materialized='table',
        alias='int_customer_orders'
    )
}}

with orders as (

    select * from {{ ref('stg_orders') }}

),

order_items as (

    select * from {{ ref('stg_order_items') }}

),

-- Aggregate order-level revenue from line items
order_revenue as (

    select
        order_id,
        sum(line_total) as order_revenue
    from order_items
    group by order_id

),

-- Customer-level aggregation
customer_orders as (

    select
        o.customer_id,

        -- Order metrics
        count(distinct o.order_id) as total_orders,
        sum(r.order_revenue) as total_revenue,
        avg(r.order_revenue) as avg_order_value,
        min(o.order_date) as first_order_date,
        max(o.order_date) as last_order_date,

        -- Recency
        {{ dbt.datediff("max(o.order_date)", "current_date()", "day") }} as days_since_last_order,

        -- Completed order count
        count(distinct case when o.status = 'completed' then o.order_id end) as completed_orders,

        -- Cancelled/refunded order count
        count(distinct case when o.status in ('cancelled', 'refunded') then o.order_id end) as failed_orders

    from orders o

    left join order_revenue r
        on o.order_id = r.order_id

    group by o.customer_id

)

select * from customer_orders
