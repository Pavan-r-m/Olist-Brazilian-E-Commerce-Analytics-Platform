-- Customer Lifetime Value Mart: Customer-level metrics

with orders as (
    select * from {{ ref('stg_orders') }}
),
payments as (
    select * from {{ ref('stg_payments') }}
),
customers as (
    select * from {{ ref('stg_customers') }}
)

select
    c.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    count(distinct o.order_id) as total_orders,
    sum(p.payment_value) as total_spent,
    min(o.order_purchase_timestamp) as first_order_date,
    max(o.order_purchase_timestamp) as last_order_date
from customers c
left join orders o on c.customer_id = o.customer_id
left join payments p on o.order_id = p.order_id
where o.order_status = 'delivered'
group by 1,2,3,4
