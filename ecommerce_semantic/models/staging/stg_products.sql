with source as (
    select * from {{ source('main', 'products') }}
)

select
    product_id,
    product_name,
    product_category,
    base_price::decimal(10, 2) as base_price,
    stock_quantity::integer     as stock_quantity
from source
