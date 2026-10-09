update stg_sales
set
  quantity = quantity + 10,
  amount_cents = amount_cents + 1000,
  amount = amount + 10,
  discount_rate = discount_rate + 0.5,
  is_priority = 1
where sale_id between 100001 and 120000
