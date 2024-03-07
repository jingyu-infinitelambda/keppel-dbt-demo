with stg_customers as (
    select
        customer_id
        , last_name
        , first_name 
        , full_name
    from {{ ref('stg_customers') }}
)

, stg_orders as (
    select
        order_id
        , customer_id
        , order_status
        , order_date
        , user_order_seq
    from {{ ref('stg_orders') }}    
)

, stg_payments as (
    select 
        payment_id
        , payment_method
        , order_id 
        , amount_dollars as order_value_dollars

    from {{ ref('stg_payments') }}
)

, final as (
    select 
        stg_orders.* exclude (customer_id)
        , stg_customers.customer_id
        , stg_customers.full_name
        , stg_customers.last_name 
        , stg_customers.first_name
        , stg_payments.payment_id
        , stg_payments.payment_method
        , stg_payments.order_value_dollars

    from stg_orders

    inner join stg_customers
        on stg_orders.customer_id = stg_customers.customer_id

    left outer join stg_payments
        on stg_orders.order_id = stg_payments.order_id
)

select * from final