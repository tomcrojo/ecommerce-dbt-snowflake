-- Assert that the sum of line item totals per order matches the order total
-- in fct_orders, within a 0.01 EUR tolerance (rounding).
-- This catches aggregation errors between the two fact tables.

with order_items_agg as (

    select
        order_id,
        sum(line_total) as computed_total
    from {{ ref('fct_order_items') }}
    group by order_id

),

orders as (

    select
        order_id,
        total_amount as reported_total
    from {{ ref('fct_orders') }}

),

mismatched as (

    select
        o.order_id,
        o.reported_total,
        ia.computed_total,
        abs(o.reported_total - ia.computed_total) as difference
    from orders o

    inner join order_items_agg ia
        on o.order_id = ia.order_id

    where abs(o.reported_total - ia.computed_total) > 0.01

)

select * from mismatched
