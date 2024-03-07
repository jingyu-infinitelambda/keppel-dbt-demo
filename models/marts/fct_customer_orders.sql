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
, customers_name as (
    select
        *
        , first_name || ' ' || last_name as name
    from customers
)

, a as (
    select
        *
        , row_number()
            over (partition by user_id order by order_date, id)
            as user_order_seq
    from orders    
)

, b as (
    select
        *
        , first_name || ' ' || last_name as name
    from customers
)

, customer_order_history as (
    select
        b.id as customer_id
        , b.name as full_name
        , b.last_name as surname
        , b.first_name as givenname
        , min(order_date) as first_order_date
        , min(case when a.status not in ('returned', 'return_pending') then order_date end) as first_non_returned_order_date
        , max(case when a.status not in ('returned', 'return_pending') then order_date end) as most_recent_non_returned_order_date
        , coalesce(max(user_order_seq), 0) as order_count
        , coalesce(count(case when a.status != 'returned' then 1 end), 0) as non_returned_order_count
        , sum(case when a.status not in ('returned', 'return_pending') then round(c.amount / 100.0, 2) else 0 end) as total_lifetime_value
        , sum(case when a.status not in ('returned', 'return_pending') then round(c.amount / 100.0, 2) else 0 end) / nullif(count(case when a.status not in ('returned', 'return_pending') then 1 end), 0) as avg_non_returned_order_value
        , array_agg(distinct a.id) as order_ids

    from a

    inner join b
        on a.user_id = b.id

    left outer join payments as c
        on a.id = c.order_id

    where a.status not in ('pending')

    group by b.id, b.name, b.last_name, b.first_name
)

-- final CTE
, final as (
    select
        orders.id as order_id
        , orders.user_id as customer_id
        , last_name as surname
        , first_name as givenname
        , first_order_date
        , order_count
        , total_lifetime_value
        , orders.status as order_status
        , payments.payment_method
        , round(amount / 100.0, 2) as order_value_dollars
    from orders 
    inner join customers_name
        on orders.user_id = customers_name.id

    inner join customer_order_history
        on orders.user_id = customer_order_history.customer_id

    left outer join payments
        on orders.id = payments.order_id
)

-- final select statment
select * from final 
