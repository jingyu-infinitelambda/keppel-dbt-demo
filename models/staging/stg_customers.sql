with customers as (
    select * from {{ source('snowflake_sample', 'raw_customers') }}
)

, final as (
    select * exclude(_row) from customers 
)

select * from final