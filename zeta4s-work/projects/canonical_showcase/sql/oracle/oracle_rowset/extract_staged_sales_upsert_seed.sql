select
  sale_id,
  store_id,
  product_id,
  customer_id,
  quantity,
  unit_price_cents,
  amount_cents,
  amount,
  discount_rate,
  is_priority,
  sold_date,
  sold_at
from stg_sales
where sale_id between 1 and 120000
order by sale_id
