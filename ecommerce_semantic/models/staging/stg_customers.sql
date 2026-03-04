with source as (
    select * from {{ source('main', 'customers') }}
)

select
    customer_id,
    email,
    first_name,
    last_name,
    payment_method_preferred,
    created_at::date as created_at
from source
