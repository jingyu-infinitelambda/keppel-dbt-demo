with

    -- import CTE
    orders_joined as (
        select * from {{ ref('int_orders_joined') }}
    ),

    -- logical CTE
    customer_order_history as (
        select
            orders_joined.customer_id,
            orders_joined.full_name,
            orders_joined.last_name,
            orders_joined.first_name,
            min(order_date) as first_order_date,
            min(
                case
                    when orders_joined.order_status not in ('returned', 'return_pending') then order_date
                end
            ) as first_non_returned_order_date,
            max(
                case
                    when orders_joined.order_status not in ('returned', 'return_pending') then order_date
                end
            ) as most_recent_non_returned_order_date,
            coalesce(max(user_order_seq), 0) as order_count,
            coalesce(
                count(case when orders_joined.order_status != 'returned' then 1 end), 0
            ) as non_returned_order_count,
            sum(
                case
                    when orders_joined.order_status not in ('returned', 'return_pending')
                    then orders_joined.amount_dollars
                    else 0
                end
            ) as total_lifetime_value,
            sum(
                case
                    when orders_joined.order_status not in ('returned', 'return_pending')
                    then orders_joined.amount_dollars
                    else 0
                end
            ) / nullif(
                count(
                    case when orders_joined.order_status not in ('returned', 'return_pending') then 1 end
                ),
                0
            ) as avg_non_returned_order_value,
            array_agg(distinct orders_joined.order_id) as order_ids

        from orders_joined

        where orders_joined.order_status not in ('pending')

        group by orders_joined.customer_id, orders_joined.full_name, orders_joined.last_name, orders_joined.first_name
),

    -- final CTE
    final as (
        select
            orders_joined.order_id,
            orders_joined.customer_id,
            orders_joined.last_name,
            orders_joined.first_name,
            first_order_date,
            order_count,
            total_lifetime_value,
            -- BUG: multiple rows per order_id in raw_payments
            orders_joined.amount_dollars as order_value_dollars,
            orders_joined.order_status,
            orders_joined.payment_id,
            orders_joined.payment_method        
        from orders_joined

        join
            customer_order_history
            on orders_joined.customer_id = customer_order_history.customer_id
    )

-- simple select statement
select *
from final
