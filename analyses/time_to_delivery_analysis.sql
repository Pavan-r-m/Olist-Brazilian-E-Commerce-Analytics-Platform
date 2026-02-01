-- Time-to-Delivery Analysis: Actual vs Estimated Delivery Time

with delivery_metrics as (
  select
    o.order_id,
    c.customer_state,
    o.order_status,
    julianday(o.order_delivered_customer_date) - julianday(o.order_purchase_timestamp) as actual_delivery_days,
    julianday(o.order_estimated_delivery_date) - julianday(o.order_purchase_timestamp) as estimated_delivery_days,
    julianday(o.order_delivered_customer_date) - julianday(o.order_estimated_delivery_date) as delivery_delay_days
  from {{ ref('stg_orders') }} o
  join {{ ref('stg_customers') }} c on o.customer_id = c.customer_id
  where o.order_delivered_customer_date is not null
    and o.order_estimated_delivery_date is not null
    and o.order_status = 'delivered'
)
select
  customer_state,
  count(*) as total_deliveries,
  round(avg(actual_delivery_days), 2) as avg_actual_delivery_days,
  round(avg(estimated_delivery_days), 2) as avg_estimated_delivery_days,
  round(avg(delivery_delay_days), 2) as avg_delay_days,
  sum(case when delivery_delay_days <= 0 then 1 else 0 end) as on_time_deliveries,
  round(sum(case when delivery_delay_days <= 0 then 1 else 0 end) * 100.0 / count(*), 2) as on_time_percentage
from delivery_metrics
group by customer_state
order by avg_delay_days desc;