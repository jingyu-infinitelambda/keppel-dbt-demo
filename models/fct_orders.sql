with orders as (
    select * from {{ ref('stg_orders') }}
)

, customers as (
    select * from {{ ref('stg_customers') }}
)

, final as (
    select 
        orders.*
        , customers.id as customer_id
        , customers.first_name
        , customers.last_name
 
    from orders 
    left join customers on orders.user_id = customers.id
)

select * from final