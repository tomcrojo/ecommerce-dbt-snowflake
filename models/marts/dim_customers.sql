{{
    config(
        materialized='table',
        alias='dim_customers'
    )
}}

with customers as (

    select * from {{ ref('stg_customers') }}

),

customer_orders as (

    select * from {{ ref('int_customer_orders') }}

),

final as (

    select
        c.customer_id,
        c.customer_name,
        c.email,
        c.country,
        c.signup_date,
        c.preferred_language,
        c.account_age_days,

        -- Order history from intermediate model
        coalesce(co.total_orders, 0) as total_orders,
        coalesce(co.total_revenue, 0) as total_revenue,
        coalesce(co.avg_order_value, 0) as avg_order_value,
        co.first_order_date,
        co.last_order_date,
        coalesce(co.days_since_last_order, 9999) as days_since_last_order,
        coalesce(co.completed_orders, 0) as completed_orders,
        coalesce(co.failed_orders, 0) as failed_orders,

        -- Customer segmentation based on order recency
        case
            when co.customer_id is null then 'new'
            when co.days_since_last_order <= 90 then 'active'
            when co.days_since_last_order <= 365 then 'returning'
            when co.days_since_last_order > 365 then 'churned'
            else 'unknown'
        end as customer_segment,

        -- Lifetime value tier based on total completed revenue
        case
            when co.total_revenue is null or co.total_revenue = 0 then 'no_orders'
            when co.total_revenue < 100 then 'bronze'
            when co.total_revenue < 500 then 'silver'
            when co.total_revenue < 2000 then 'gold'
            else 'platinum'
        end as lifetime_value_tier,

        current_timestamp as _loaded_at

    from customers c

    left join customer_orders co
        on c.customer_id = co.customer_id

)

select * from final
