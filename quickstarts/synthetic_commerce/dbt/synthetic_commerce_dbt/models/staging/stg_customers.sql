with source as (

    select * from {{ source('raw', 'raw_customers') }}

)

select
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    city,
    state,
    cast(signup_date as date) as signup_date,
    lifetime_value,
    is_active

from source
