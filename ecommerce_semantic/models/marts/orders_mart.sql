with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select
        customer_id,
        first_name,
        last_name,
        customer_segment
    from {{ ref('customers_mart') }}
),

order_items_agg as (
    select
        order_id,
        count(order_item_id)  as item_count,
        sum(quantity)         as total_quantity
    from {{ ref('stg_order_items') }}
    group by order_id
),

final as (
    select
        o.order_id,
        o.customer_id,
        c.first_name,
        c.last_name,
        c.customer_segment,
        o.purchased_at,
        o.order_status,
        o.shipped_at,
        o.delivered_at,
        o.shipping_address,
        o.order_total,
        coalesce(oi.item_count, 0)      as item_count,
        coalesce(oi.total_quantity, 0)  as total_quantity,
        datediff('day', o.purchased_at, o.shipped_at)    as days_to_ship,
        datediff('day', o.shipped_at, o.delivered_at)    as days_to_deliver
    from orders o
    left join customers c       on o.customer_id = c.customer_id
    left join order_items_agg oi on o.order_id   = oi.order_id
)

select * from final
