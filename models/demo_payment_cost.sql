with payments as (
    select * from {{ source('snowflake_sample', 'raw_payments') }}
)

, payment_cost as (
    select * from {{ ref('payment_method_cost') }}
)

, final as (
    select 
        payments.*
        , payment_cost.cost_per_transaction_cents as transaction_cost

    from payments
    left join payment_cost using (payment_method)
)

select * from final