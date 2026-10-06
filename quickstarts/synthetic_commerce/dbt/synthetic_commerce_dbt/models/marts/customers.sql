with customers as (

    select * from {{ ref('stg_customers') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

customer_orders as (

    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders,
        sum(total) as lifetime_order_value

    from orders
    group by customer_id

)

select
    customers.customer_id,
    customers.first_name,
    customers.last_name,
    customers.email,
    customers.city,
    customers.state,
    customers.signup_date,
    customers.is_active,
    customer_orders.first_order_date,
    customer_orders.most_recent_order_date,
    coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
    coalesce(customer_orders.lifetime_order_value, 0) as lifetime_order_value

from customers
left join customer_orders using (customer_id)
