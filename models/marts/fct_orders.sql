{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
)

, customers as (
    select * from {{ ref('stg_customers') }}
)

, final as (
    select 
        orders.*
        , customers.first_name
        , customers.last_name
 
    from orders 
    left join customers using (customer_id)
)

select * from final