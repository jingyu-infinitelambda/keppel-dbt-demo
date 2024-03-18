with customers as (
    select * from {{ source('snowflake_sample', 'raw_customers') }}
)

, final as (
    select 
        id as customer_id
        , first_name
        , last_name
        , first_name || ' ' || last_name as full_name
    
    from customers 
)

select * from final