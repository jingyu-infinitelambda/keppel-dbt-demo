with payments as (
    select * from {{ source('snowflake_sample', 'raw_payments') }}
)

, final as (
    select 
        id as payment_id 
        , order_id
        , amount as amount_cents
        , payment_method  
    
    from payments 
)

select * from final