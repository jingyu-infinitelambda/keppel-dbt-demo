with orders as (
    select * from {{ source('snowflake_sample', 'raw_orders') }}
)

, final as (
    select * from orders
)

select * from final