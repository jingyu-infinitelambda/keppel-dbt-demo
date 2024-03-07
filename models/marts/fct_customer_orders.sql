-- import CTE
with orders as (
    select * from {{ source('snowflake_sample', 'raw_orders') }}
)

, customers as (
    select * from {{ source('snowflake_sample', 'raw_customers') }}
)

, payments as (
    select * from {{ source('snowflake_sample', 'raw_payments') }}
)

-- logical CTE

-- staging customers
, stg_customers as (
    select
        id as customer_id
        , last_name
        , first_name 
        , first_name || ' ' || last_name as full_name
    from customers
)

-- staging orders
, stg_orders as (
    select
        id as order_id
        , user_id as customer_id
        , status as order_status
        , order_date
        , row_number()
            over (partition by user_id order by order_date, id)
            as user_order_seq
    from orders    
)

-- staging payments 
, stg_payments as (
    select 
        id as payment_id
        , payment_method
        , order_id 
        , round(amount / 100.0, 2) as order_value_dollars

    from payments 
)

, customer_order_history as (
    select
        stg_customers.customer_id
        , stg_customers.full_name
        , stg_customers.last_name 
        , stg_customers.first_name
        , min(order_date) as first_order_date
        , min(case when stg_orders.order_status not in ('returned', 'return_pending') then order_date end) as first_non_returned_order_date
        , max(case when stg_orders.order_status not in ('returned', 'return_pending') then order_date end) as most_recent_non_returned_order_date
        , coalesce(max(user_order_seq), 0) as order_count
        , coalesce(count(case when stg_orders.order_status != 'returned' then 1 end), 0) as non_returned_order_count
        , sum(case when stg_orders.order_status not in ('returned', 'return_pending') then stg_payments.order_value_dollars else 0 end) as total_lifetime_value
        , sum(case when stg_orders.order_status not in ('returned', 'return_pending') then stg_payments.order_value_dollars else 0 end) / nullif(count(case when stg_orders.order_status not in ('returned', 'return_pending') then 1 end), 0) as avg_non_returned_order_value
        , array_agg(distinct stg_orders.order_id) as order_ids

    from stg_orders

    inner join stg_customers
        on stg_orders.customer_id = stg_customers.customer_id

    left outer join stg_payments
        on stg_orders.order_id = stg_payments.order_id

    where stg_orders.order_status not in ('pending')

    group by stg_customers.customer_id, stg_customers.full_name, stg_customers.last_name, stg_customers.first_name
)

-- final CTE
, final as (
    select
        stg_orders.order_id
        , stg_orders.customer_id
        , stg_customers.last_name 
        , stg_customers.first_name
        , first_order_date
        , order_count
        , total_lifetime_value
        , stg_orders.order_status
        , stg_payments.payment_id
        , stg_payments.payment_method
        , stg_payments.order_value_dollars
    from stg_orders 
    inner join stg_customers
        on stg_orders.customer_id = stg_customers.customer_id

    inner join customer_order_history
        on stg_orders.customer_id = customer_order_history.customer_id

    left outer join stg_payments
        on stg_orders.order_id = stg_payments.order_id
)

-- final select statment
select * from final 
