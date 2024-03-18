-- import CTE
with
    base_orders as (select * from {{ source("snowflake_sample", "raw_orders") }}),

    base_customers as (select * from {{ source("snowflake_sample", "raw_customers") }}),

    base_payments as (select * from {{ source("snowflake_sample", "raw_payments") }}),

    -- logical CTEs
    stg_orders as (
        select 
            id as order_id,
            user_id as customer_id,
            status as order_status,
            order_date,
            row_number() over (
                partition by user_id order by order_date, id
            ) as user_order_seq

        from base_orders
    ),

    stg_customers as (
        select 
            id as customer_id,
            first_name,
            last_name,
            first_name || ' ' || last_name as full_name
        from base_customers
    ),

    stg_payments as (
        select 
            id as payment_id,
            order_id,
            round(amount / 100.0, 2) as amount,
            payment_method

        from base_payments
    ),

    customer_order_history as (
        select
            stg_customers.customer_id,
            stg_customers.full_name,
            stg_customers.last_name,
            stg_customers.first_name,
            min(order_date) as first_order_date,
            min(
                case
                    when stg_orders.order_status not in ('returned', 'return_pending') then order_date
                end
            ) as first_non_returned_order_date,
            max(
                case
                    when stg_orders.order_status not in ('returned', 'return_pending') then order_date
                end
            ) as most_recent_non_returned_order_date,
            coalesce(max(user_order_seq), 0) as order_count,
            coalesce(
                count(case when stg_orders.order_status != 'returned' then 1 end), 0
            ) as non_returned_order_count,
            sum(
                case
                    when stg_orders.order_status not in ('returned', 'return_pending')
                    then stg_payments.amount
                    else 0
                end
            ) as total_lifetime_value,
            sum(
                case
                    when stg_orders.order_status not in ('returned', 'return_pending')
                    then stg_payments.amount
                    else 0
                end
            ) / nullif(
                count(
                    case when stg_orders.order_status not in ('returned', 'return_pending') then 1 end
                ),
                0
            ) as avg_non_returned_order_value,
            array_agg(distinct stg_orders.order_id) as order_ids

        from stg_orders

        join stg_customers on stg_orders.customer_id = stg_customers.customer_id

        left outer join stg_payments on stg_orders.order_id = stg_payments.order_id

        where stg_orders.order_status not in ('pending')

        group by stg_customers.customer_id, stg_customers.full_name, stg_customers.last_name, stg_customers.first_name
    ),

    -- final CTE
    final as (
        select
            orders.order_id,
            orders.customer_id,
            stg_customers.last_name,
            stg_customers.first_name,
            first_order_date,
            order_count,
            total_lifetime_value,
            stg_payments.amount as order_value_dollars,
            orders.order_status,
            stg_payments.payment_id,
            stg_payments.payment_method
        from stg_orders as orders

        join stg_customers on orders.customer_id = stg_customers.customer_id

        join
            customer_order_history
            on orders.customer_id = customer_order_history.customer_id

        left outer join stg_payments on orders.order_id = stg_payments.order_id
    )

select *
from final
