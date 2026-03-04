with source as (
    select * from {{ source('main', 'orders') }}
)

select
    order_id,
    customer_id,
    purchase_date::timestamp as purchased_at,
    order_status,
    shipped_date::timestamp  as shipped_at,
    delivered_date::timestamp as delivered_at,
    shipping_address,
    order_total::decimal(10, 2) as order_total
from source
