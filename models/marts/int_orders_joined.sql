-- import CTE
with
    stg_orders as (
        select order_id, customer_id, order_status, order_date, user_order_seq

        from {{ ref("stg_orders") }}
    ),

    stg_customers as (
        select customer_id, first_name, last_name, full_name
        from {{ ref("stg_customers") }}
    ),

    stg_payments as (
        select payment_id, order_id, amount_dollars, payment_method

        from {{ ref("stg_payments") }}
    ),

    -- final CTEs
    final as (
        select
            stg_customers.customer_id,
            stg_customers.full_name,
            stg_customers.last_name,
            stg_customers.first_name,
            stg_orders.order_id,
            stg_orders.order_date,
            stg_orders.order_status,
            stg_orders.user_order_seq,
            stg_payments.amount_dollars,
            stg_payments.payment_id,
            stg_payments.payment_method

        from stg_orders

        join stg_customers on stg_orders.customer_id = stg_customers.customer_id

        left outer join stg_payments on stg_orders.order_id = stg_payments.order_id
    )

select *
from final
