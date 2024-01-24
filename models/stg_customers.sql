with customers as (
    select * from {{ source('snowflake_sample', 'raw_customers') }}
)

, final as (
    select * from customers 
)

select * from final