select 
    orders.id as order_id,
    orders.user_id as customer_id,
    customers.last_name,
    customers.first_name,
    first_order_date,
    order_count,
    total_lifetime_value,
    round(amount/100.0,2) as order_value_dollars,
    orders.status as order_status,
    payments.id as payment_id,
    payments.payment_method
from {{ source('snowflake_sample', 'raw_orders') }} as orders

join (
      select 
        first_name || ' ' || last_name as name, 
        * 
      from {{ source('snowflake_sample', 'raw_customers') }}
) customers
on orders.user_id = customers.id

join (

    select 
        b.id as customer_id,
        b.name as full_name,
        b.last_name,
        b.first_name,
        min(order_date) as first_order_date,
        min(case when a.status NOT IN ('returned','return_pending') then order_date end) as first_non_returned_order_date,
        max(case when a.status NOT IN ('returned','return_pending') then order_date end) as most_recent_non_returned_order_date,
        COALESCE(max(user_order_seq),0) as order_count,
        COALESCE(count(case when a.status != 'returned' then 1 end),0) as non_returned_order_count,
        sum(case when a.status NOT IN ('returned','return_pending') then ROUND(c.amount/100.0,2) else 0 end) as total_lifetime_value,
        sum(case when a.status NOT IN ('returned','return_pending') then ROUND(c.amount/100.0,2) else 0 end)/NULLIF(count(case when a.status NOT IN ('returned','return_pending') then 1 end),0) as avg_non_returned_order_value,
        array_agg(distinct a.id) as order_ids

    from (
      select 
        row_number() over (partition by user_id order by order_date, id) as user_order_seq,
        *
      from {{ source('snowflake_sample', 'raw_orders') }}
    ) a

    join ( 
      select 
        first_name || ' ' || last_name as name, 
        * 
      from {{ source('snowflake_sample', 'raw_customers') }}
    ) b
    on a.user_id = b.id

    left outer join {{ source('snowflake_sample', 'raw_payments') }} c
    on a.id = c.order_id

    where a.status NOT IN ('pending') 

    group by b.id, b.name, b.last_name, b.first_name

) customer_order_history
on orders.user_id = customer_order_history.customer_id

left outer join {{ source('snowflake_sample', 'raw_payments') }} payments
on orders.id = payments.order_id
