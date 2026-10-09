alter table source.sales
update
  quantity = quantity + 10,
  amount_cents = amount_cents + 1000,
  amount = amount + 10,
  discount_rate = discount_rate + 0.5,
  is_priority = true
where sale_id between 100001 and 120000
settings mutations_sync = 1
