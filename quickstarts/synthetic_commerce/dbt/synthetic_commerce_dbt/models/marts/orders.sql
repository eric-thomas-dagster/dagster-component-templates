with orders as (

    select * from {{ ref('stg_orders') }}

),

order_sequence as (

    select
        *,
        row_number() over (partition by customer_id order by order_date) as customer_order_seq

    from orders

)

select
    order_id,
    customer_id,
    order_date,
    category,
    num_items,
    subtotal,
    shipping,
    tax,
    total,
    status,
    region,
    customer_order_seq = 1 as is_first_order

from order_sequence
