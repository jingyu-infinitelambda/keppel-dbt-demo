with orders as (
    select * from {{ source('snowflake_sample', 'raw_orders') }}
)

, final as (
    select 
        id as order_id
        , user_id as customer_id
        , status as order_status
        , order_date
    
    from orders
)

select * from final