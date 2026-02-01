-- Orders Mart: Orders joined with customers, payments, sellers, and order items

with orders as (
    select * from {{ ref('stg_orders') }}
),
customers as (
    select * from {{ ref('stg_customers') }}
),
payments as (
    select order_id, sum(payment_value) as total_payment, count(*) as payment_count
    from {{ ref('stg_payments') }}
    group by order_id
),
sellers as (
    select * from {{ ref('stg_sellers') }}
),
order_items as (
    select order_id, sum(price) as total_items_value, count(*) as items_count
    from {{ ref('stg_order_items') }}
    group by order_id
)

select
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    p.total_payment,
    p.payment_count,
    oi.total_items_value,
    oi.items_count
from orders o
left join customers c on o.customer_id = c.customer_id
left join payments p on o.order_id = p.order_id
left join order_items oi on o.order_id = oi.order_id
