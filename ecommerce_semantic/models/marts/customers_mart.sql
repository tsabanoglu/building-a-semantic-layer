with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

order_stats as (
    select
        customer_id,
        count(order_id)                                  as total_orders,
        min(purchased_at)                                as first_order_at,
        max(purchased_at)                                as last_order_at,
        sum(order_total)                                 as lifetime_value
    from orders
    group by customer_id
),

final as (
    select
        c.customer_id,
        c.email,
        c.first_name,
        c.last_name,
        c.payment_method_preferred,
        c.created_at,
        coalesce(os.total_orders, 0)                     as total_orders,
        os.first_order_at,
        os.last_order_at,
        coalesce(os.lifetime_value, 0.0)                 as lifetime_value,

        case
            when os.total_orders is null then 'no_orders'
            when os.total_orders = 1     then 'new'
            when datediff('day', os.last_order_at, (select max(purchased_at) from orders)) > 90 then 'churned'
            else 'repeat'
        end                                              as customer_segment

    from customers c
    left join order_stats os on c.customer_id = os.customer_id
)

select * from final
