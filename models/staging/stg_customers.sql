{{
    config(
        materialized='view',
        alias='stg_customers'
    )
}}

with source as (

    select * from {{ source('raw', 'raw_customers') }}

),

renamed as (

    select
        customer_id,
        trim(customer_name) as customer_name,
        lower(trim(email)) as email,

        -- Standardize country codes: uppercase, trim whitespace
        upper(trim(country)) as country,

        cast(signup_date as date) as signup_date,

        lower(trim(preferred_language)) as preferred_language,

        -- Derived: days since signup, useful for customer segmentation
        {% if target.type == 'postgres' %}
        (current_date - signup_date) as account_age_days
        {% else %}
        datediff('day', signup_date, current_date()) as account_age_days
        {% endif %}

    from source

)

select * from renamed
