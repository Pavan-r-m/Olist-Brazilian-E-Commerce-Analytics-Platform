-- Product Recommendation Engine: Frequently Bought Together

with product_pairs as (
  select
    a.product_id as product_a,
    b.product_id as product_b,
    count(*) as times_bought_together
  from {{ ref('stg_order_items') }} a
  join {{ ref('stg_order_items') }} b 
    on a.order_id = b.order_id 
    and a.product_id < b.product_id
  group by a.product_id, b.product_id
  having count(*) >= 3
),
product_info as (
  select
    pp.product_a,
    pa.product_category_name as category_a,
    pp.product_b,
    pb.product_category_name as category_b,
    pp.times_bought_together
  from product_pairs pp
  left join {{ ref('stg_products') }} pa on pp.product_a = pa.product_id
  left join {{ ref('stg_products') }} pb on pp.product_b = pb.product_id
)
select
  product_a,
  category_a,
  product_b,
  category_b,
  times_bought_together,
  case
    when category_a = category_b then 'Same Category'
    else 'Cross-Category'
  end as recommendation_type
from product_info
order by times_bought_together desc
limit 50;