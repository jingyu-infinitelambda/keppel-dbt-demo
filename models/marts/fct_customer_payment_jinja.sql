{% set payment_methods_query %}
    select distinct payment_method from {{ ref('stg_payments') }}
{% endset %}

{% set results = run_query(payment_methods_query) %}

{% if execute %}
    {# Return the first column #}
    {% set payment_methods = results.columns[0].values() %}
{% else %}
    {% set payment_methods = [] %}
{% endif %}

with orders as (
    select * from {{ ref('stg_orders') }}
)

, payment as (
    select * from {{ ref('stg_payments') }}
)

, order_payment as (
    select 
        orders.*
        , payment.payment_method

    from orders 
    left join payment using (order_id)
)

, final as (
    select 
        customer_id
        , count(1) as cnt_orders 
        
        {% for payment_method in payment_methods %}
        , count_if(payment_method = '{{payment_method}}') as cnt_orders_{{payment_method}}
        {% endfor %}

    from order_payment
    group by 1
)

select * from final