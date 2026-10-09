select
  sale_id,
  store_id,
  product_id,
  customer_id,
  quantity + 10 as quantity,
  unit_price_cents,
  amount_cents + (10 * unit_price_cents) as amount_cents,
  amount + (toDecimal64(10, 2) * toDecimal64(unit_price_cents, 2) / 100) as amount,
  discount_rate,
  true as is_priority,
  sold_date,
  sold_at
from mart.stg_es_sales
where sale_id between 2 and 3
order by sale_id
