with orders as (
    select * from {{ source('snowflake_sample', 'raw_orders') }}
)

, final as (
    select * exclude(_row) from orders
)

select * from final