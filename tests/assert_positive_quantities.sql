-- Assert that all quantities in fct_order_items are positive (> 0).
-- A quantity of zero or negative indicates data quality issues upstream.

select
    order_item_id,
    order_id,
    product_id,
    quantity
from {{ ref('fct_order_items') }}
where quantity <= 0
