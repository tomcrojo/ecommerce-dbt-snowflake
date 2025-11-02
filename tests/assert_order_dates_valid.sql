-- Assert that no order dates are in the future or before 2020.
-- This catches data entry errors and pipeline issues that produce implausible dates.

select
    order_id,
    order_date
from {{ ref('fct_orders') }}
where order_date > current_date()
   or order_date < '2020-01-01'
