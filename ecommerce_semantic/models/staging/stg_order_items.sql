with source as (
    select * from {{ source('main', 'order_items') }}
)

select
    order_item_id,
    order_id,
    product_id,
    quantity::integer        as quantity,
    unit_price::decimal(10, 2) as unit_price,
    line_total::decimal(10, 2) as line_total
from source
