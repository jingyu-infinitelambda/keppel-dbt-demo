with customers as (
    select * from {{ source('snowflake_sample', 'customer') }}
)

, final as (
    select 
        c_custkey as customer_key 
        , c_name as customer_name
        , c_nationkey as nation_key 
        , c_mktsegment as marketing_segment
        , c_comment as comment 

    from customers 
)

select * from final