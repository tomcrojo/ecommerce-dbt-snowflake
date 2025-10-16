{{
    config(
        materialized='view',
        alias='stg_orders'
    )
}}

with source as (

    select * from {{ source('raw', 'raw_orders') }}

),

renamed as (

    select
        order_id,
        customer_id,
        cast(order_date as date) as order_date,
        lower(trim(status)) as status,
        trim(region) as region,
        upper(trim(currency)) as currency,
        year(order_date) as order_year,
        month(order_date) as order_month

    from source

    -- Filter out test orders (status is a placeholder for any test-pattern logic)
    where lower(trim(status)) not in ('test', 'sample')

)

select * from renamed
