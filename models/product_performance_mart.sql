-- Product Performance Mart: Product-level sales and performance

with order_items as (
    select * from {{ ref('stg_order_items') }}
),
products as (
    select * from {{ ref('stg_products') }}
),
orders as (
    select order_id, order_status from {{ ref('stg_orders') }}
)

select
    p.product_id,
    p.product_category_name,
    p.product_name_lenght,
    p.product_description_lenght,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    count(oi.order_id) as total_orders,
    sum(oi.price) as total_sales_value,
    sum(oi.freight_value) as total_freight_value
from products p
left join order_items oi on p.product_id = oi.product_id
left join orders o on oi.order_id = o.order_id
where o.order_status = 'delivered'
group by 1,2,3,4,5,6,7,8,9
