{% snapshot scd2_customers %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='signup_date',
        invalidate_hard_deletes=True
    )
}}

-- SCD Type 2 snapshot on staging customers.
-- Tracks changes to email, country, and customer_name.
-- Uses updated_at strategy — any change to the source record's signup_date
-- column (or any column, since dbt hashes the full row) triggers a new version.

select * from {{ ref('stg_customers') }}

{% endsnapshot %}
