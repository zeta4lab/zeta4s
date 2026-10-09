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
from source.sales
where sale_id between 120001 and 140000
order by sale_id
