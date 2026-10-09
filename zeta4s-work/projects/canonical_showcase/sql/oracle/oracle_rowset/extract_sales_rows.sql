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
from sales
order by sale_id
