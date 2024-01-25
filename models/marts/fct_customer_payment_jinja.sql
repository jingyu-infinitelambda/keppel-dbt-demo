{% set payment_methods = ["bank_transfer", "credit_card", "gift_card","coupon"] %}

with orders as (
    select * from {{ ref('stg_orders') }}
)

, payment as (
    select * from {{ ref('stg_payments') }}
)

, order_payment as (
    select 
        orders.*
        , payment.payment_method

    from orders 
    left join payment using (order_id)
)

, final as (
    select 
        customer_id
        , count(1) as cnt_orders 
        
        {% for payment_method in payment_methods %}
        , sum(case when payment_method = '{{payment_method}}' then amount end) as cnt_orders_{{payment_method}}
        {% endfor %}

    from order_payment
    group by 1
)

select * from final