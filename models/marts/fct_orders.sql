{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where order_date > (select max(order_date) from {{ this }}) 
    {% endif %}
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