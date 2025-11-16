{{
    config(
        materialized='table',
        alias='rpt_daily_revenue'
    )
}}

with order_items as (

    select * from {{ ref('fct_order_items') }}

),

daily_revenue as (

    select
        order_date as report_date,
        region,
        product_category,
        order_year,
        order_month,

        -- Volume metrics
        count(distinct order_id) as total_orders,
        sum(quantity) as total_items_sold,

        -- Revenue metrics (only completed orders)
        sum(case when is_revenue_recognized then line_total else 0 end) as total_revenue,

        -- Average order value for the day/region/category
        case
            when count(distinct order_id) > 0
            then sum(line_total) / count(distinct order_id)
            else 0
        end as avg_order_value,

        -- Customer reach
        count(distinct customer_id) as unique_customers,

        current_timestamp as _loaded_at

    from order_items

    group by
        order_date,
        region,
        product_category,
        order_year,
        order_month

)

select * from daily_revenue
order by report_date desc, region, product_category
