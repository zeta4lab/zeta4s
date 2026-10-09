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
from mart.stg_es_sales
where sale_id <= 3
order by sale_id
