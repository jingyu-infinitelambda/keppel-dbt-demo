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
        , count_if(payment_method='credit_card') as cnt_orders_credit_card
        , count_if(payment_method='coupon') as cnt_orders_coupon
        , count_if(payment_method='bank_transfer') as cnt_orders_bank_transfer
        , count_if(payment_method='gift_card') as cnt_orders_gift_card

    from order_payment
    group by 1
)

select * from final