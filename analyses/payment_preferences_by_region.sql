-- Payment Method Preferences by Region

with payment_by_region as (
  select
    c.customer_state,
    p.payment_type,
    count(distinct o.order_id) as order_count,
    sum(p.payment_value) as total_payment_value,
    round(avg(p.payment_value), 2) as avg_payment_value
  from {{ ref('stg_orders') }} o
  join {{ ref('stg_customers') }} c on o.customer_id = c.customer_id
  join {{ ref('stg_payments') }} p on o.order_id = p.order_id
  group by c.customer_state, p.payment_type
),
state_totals as (
  select
    customer_state,
    sum(order_count) as state_total_orders
  from payment_by_region
  group by customer_state
)
select
  pbr.customer_state,
  pbr.payment_type,
  pbr.order_count,
  round(pbr.total_payment_value, 2) as total_payment_value,
  pbr.avg_payment_value,
  round(pbr.order_count * 100.0 / st.state_total_orders, 2) as percentage_of_state_orders
from payment_by_region pbr
join state_totals st on pbr.customer_state = st.customer_state
order by pbr.customer_state, pbr.order_count desc;