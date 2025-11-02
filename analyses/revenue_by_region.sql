-- Analysis: Revenue by Spanish region with month-over-month growth and ranking.
-- Run manually with: dbt compile --select revenue_by_region && execute the compiled SQL.

with monthly_revenue as (

    select
        region,
        order_year,
        order_month,
        sum(line_total) as monthly_revenue,
        count(distinct order_id) as total_orders,
        sum(quantity) as total_items,
        count(distinct customer_id) as unique_customers

    from {{ ref('fct_order_items') }}
    where order_status = 'completed'

    group by
        region,
        order_year,
        order_month

),

with_growth as (

    select
        region,
        order_year,
        order_month,
        monthly_revenue,
        total_orders,
        total_items,
        unique_customers,

        -- Month-over-month revenue change
        lag(monthly_revenue) over (
            partition by region
            order by order_year, order_month
        ) as prev_month_revenue,

        case
            when lag(monthly_revenue) over (
                partition by region
                order by order_year, order_month
            ) > 0
            then round(
                (monthly_revenue - lag(monthly_revenue) over (
                    partition by region
                    order by order_year, order_month
                )) / lag(monthly_revenue) over (
                    partition by region
                    order by order_year, order_month
                ) * 100,
                2
            )
            else null
        end as mom_growth_pct,

        -- Region rank by revenue within each month
        dense_rank() over (
            partition by order_year, order_month
            order by monthly_revenue desc
        ) as region_revenue_rank

    from monthly_revenue

)

select
    region,
    order_year,
    order_month,
    monthly_revenue,
    prev_month_revenue,
    mom_growth_pct,
    region_revenue_rank,
    total_orders,
    total_items,
    unique_customers

from with_growth

order by order_year desc, order_month desc, region_revenue_rank asc
