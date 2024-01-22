with orders as (
    select * from {{ ref('stg_orders') }}
)

, customers as (
    select * from {{ ref('stg_customers') }}
)

, final as (
    select 
        orders.*
        , customers.customer_name 
        , customers.marketing_segment as customer_mkt_segment
 
    from orders 
    left join customers using (customer_key)
)

select * from final