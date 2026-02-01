-- Customer Segmentation: High-value, At-risk, and Loyal Customers

with customer_metrics as (
  select
    customer_unique_id,
    customer_city,
    customer_state,
    total_spent,
    total_orders,
    julianday('now') - julianday(last_order_date) as days_since_last_order,
    julianday(last_order_date) - julianday(first_order_date) as customer_lifetime_days
  from {{ ref('customer_lifetime_value_mart') }}
),
segmentation as (
  select
    customer_unique_id,
    customer_city,
    customer_state,
    total_spent,
    total_orders,
    days_since_last_order,
    customer_lifetime_days,
    case
      when total_spent > 1000 and days_since_last_order < 90 then 'High-Value Active'
      when total_spent > 1000 and days_since_last_order >= 90 then 'High-Value At-Risk'
      when total_orders >= 3 and days_since_last_order < 180 then 'Loyal'
      when days_since_last_order > 180 then 'Churned'
      when total_orders = 1 then 'One-Time Buyer'
      else 'Regular'
    end as customer_segment
  from customer_metrics
)
select
  customer_segment,
  count(*) as customer_count,
  round(avg(total_spent), 2) as avg_spent,
  round(avg(total_orders), 2) as avg_orders,
  round(avg(days_since_last_order), 2) as avg_days_since_last_order
from segmentation
group by customer_segment
order by customer_count desc;