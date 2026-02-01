-- Geographic Sales Heatmap: Sales and Orders by State

with state_sales as (
  select
    c.customer_state,
    c.customer_city,
    count(distinct o.order_id) as total_orders,
    sum(o.total_items_value) as total_sales,
    count(distinct o.customer_id) as unique_customers
  from {{ ref('orders_mart') }} o
  join {{ ref('stg_customers') }} c on o.customer_id = c.customer_id
  group by c.customer_state, c.customer_city
)
select
  customer_state,
  count(distinct customer_city) as cities_count,
  sum(total_orders) as total_orders,
  round(sum(total_sales), 2) as total_sales,
  sum(unique_customers) as unique_customers,
  round(sum(total_sales) / sum(total_orders), 2) as avg_order_value,
  round(sum(total_sales) / sum(unique_customers), 2) as avg_customer_value
from state_sales
group by customer_state
order by total_sales desc;