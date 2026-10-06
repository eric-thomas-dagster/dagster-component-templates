with source as (

    select * from {{ source('raw', 'raw_orders') }}

)

select
    order_id,
    customer_id,
    cast(order_date as timestamp) as order_date,
    category,
    num_items,
    subtotal,
    shipping,
    tax,
    total,
    status,
    region

from source
