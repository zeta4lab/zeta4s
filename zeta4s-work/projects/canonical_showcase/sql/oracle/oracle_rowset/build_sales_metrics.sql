create table sales_metrics as
select
  store_id,
  product_id,
  count(*) as sale_count,
  sum(quantity) as total_quantity,
  sum(amount_cents) as total_amount_cents,
  sum(amount) as total_amount,
  avg(discount_rate) as avg_discount_rate,
  sum(is_priority) as priority_sale_count,
  min(sold_date) as first_sold_date,
  max(sold_date) as last_sold_date,
  min(sold_at) as first_sold_at,
  max(sold_at) as last_sold_at
from stg_sales
group by
  store_id,
  product_id
