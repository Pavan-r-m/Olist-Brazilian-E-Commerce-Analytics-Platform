-- Seasonal Trends: Sales and Orders by Month and Quarter

with monthly_sales as (
  select
    strftime(order_purchase_timestamp, '%Y') as year,
    strftime(order_purchase_timestamp, '%m') as month,
    cast(strftime(order_purchase_timestamp, '%m') as integer) as month_num,
    case
      when cast(strftime(order_purchase_timestamp, '%m') as integer) in (1,2,3) then 'Q1'
      when cast(strftime(order_purchase_timestamp, '%m') as integer) in (4,5,6) then 'Q2'
      when cast(strftime(order_purchase_timestamp, '%m') as integer) in (7,8,9) then 'Q3'
      else 'Q4'
    end as quarter,
    count(distinct order_id) as total_orders,
    sum(total_items_value) as total_sales
  from {{ ref('orders_mart') }}
  group by year, month, month_num, quarter
)
select
  year,
  quarter,
  month,
  total_orders,
  round(total_sales, 2) as total_sales,
  round(total_sales / total_orders, 2) as avg_order_value,
  round(lag(total_sales) over (order by year, month_num), 2) as prev_month_sales,
  round((total_sales - lag(total_sales) over (order by year, month_num)) * 100.0 / 
        nullif(lag(total_sales) over (order by year, month_num), 0), 2) as mom_growth_pct
from monthly_sales
order by year, month_num;