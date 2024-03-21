{{
    config(
        materialized='incremental',
        unique_key='customer_id'
    )
}}

with customers as (
    select * from {{ ref('stg_customers') }}

    {% if is_incremental() %}
        where customer_id in (select customer_id from {{ this }}) 
    {% endif %}
)

, final as (
    select
        customers.customer_id
        , customers.first_name
        , customers.last_name
        , sysdate() as created_at

    from customers
)

select * from final
