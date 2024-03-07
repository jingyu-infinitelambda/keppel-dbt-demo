-- import CTE
with orders_joined as (
    select * from {{ ref('int_orders_joined') }}
)

-- logical CTE
, customer_order_history as (
    select
        customer_id 
        , full_name
        , last_name 
        , first_name
        , min(order_date) as first_order_date
        , min(case when order_status not in ('returned', 'return_pending') then order_date end) as first_non_returned_order_date
        , max(case when order_status not in ('returned', 'return_pending') then order_date end) as most_recent_non_returned_order_date
        , coalesce(max(user_order_seq), 0) as order_count
        , coalesce(count(case when order_status != 'returned' then 1 end), 0) as non_returned_order_count
        , sum(case when order_status not in ('returned', 'return_pending') then order_value_dollars else 0 end) as total_lifetime_value
        , sum(case when order_status not in ('returned', 'return_pending') then order_value_dollars else 0 end) / nullif(count(case when order_status not in ('returned', 'return_pending') then 1 end), 0) as avg_non_returned_order_value
        , array_agg(distinct order_id) as order_ids

    from orders_joined

    where order_status not in ('pending')

    group by customer_id, full_name, last_name, first_name
)

-- final CTE
, final as (
    select
        orders_joined.order_id
        , orders_joined.customer_id
        , orders_joined.last_name 
        , orders_joined.first_name
        , customer_order_history.first_order_date
        , customer_order_history.order_count
        , customer_order_history.total_lifetime_value
        , orders_joined.order_status
        , orders_joined.payment_id
        , orders_joined.payment_method
        , orders_joined.order_value_dollars
    from orders_joined 
    
    inner join customer_order_history
        on orders_joined.customer_id = customer_order_history.customer_id
)

-- final select statment
select * from final 
