with products as (
    select * from {{ ref('stg_products') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select order_id, order_status from {{ ref('stg_orders') }}
),

product_sales as (
    select
        oi.product_id,
        count(distinct oi.order_id)   as total_orders,
        sum(oi.quantity)              as total_units_sold,
        sum(oi.line_total)            as total_revenue,
        avg(oi.unit_price)            as avg_selling_price
    from order_items oi
    inner join orders o on oi.order_id = o.order_id
    where o.order_status != 'cancelled'
    group by oi.product_id
),

final as (
    select
        p.product_id,
        p.product_name,
        p.product_category,
        p.base_price,
        p.stock_quantity,
        coalesce(ps.total_orders, 0)       as total_orders,
        coalesce(ps.total_units_sold, 0)   as total_units_sold,
        coalesce(ps.total_revenue, 0.0)    as total_revenue,
        ps.avg_selling_price
    from products p
    left join product_sales ps on p.product_id = ps.product_id
)

select * from final
