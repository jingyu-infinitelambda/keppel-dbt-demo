with

    -- import CTE
    stg_customers as (select * from {{ ref("stg_customers") }}),

    stg_orders as (select * from {{ ref("stg_orders") }}),

    stg_payments as (select * from {{ ref("stg_payments") }}),

    orders_joined as (
        select
            stg_customers.customer_id,
            stg_customers.full_name,
            stg_customers.last_name,
            stg_customers.first_name,
            stg_orders.order_id,
            stg_orders.order_date,
            stg_orders.order_status,
            stg_orders.user_order_seq,
            stg_payments.payment_id,
            stg_payments.payment_method,
            stg_payments.amount_dollars

        from stg_orders
        join stg_customers on stg_orders.customer_id = stg_customers.customer_id
        left outer join stg_payments on stg_orders.order_id = stg_payments.order_id
    )

select *
from orders_joined
