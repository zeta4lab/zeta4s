create table source.sales
engine = MergeTree()
order by sale_id
as
select
  number + 1 as sale_id,
  toUInt16((number % 50) + 1) as store_id,
  toUInt32((number % 10000) + 1) as product_id,
  toString(concat('customer-', toString((number % 250000) + 1))) as customer_id,
  toUInt8((number % 5) + 1) as quantity,
  toUInt32((number % 20000) + 100) as unit_price_cents,
  toUInt64(((number % 5) + 1) * ((number % 20000) + 100)) as amount_cents,
  toDecimal64(((number % 5) + 1) * ((number % 20000) + 100), 2) as amount,
  toFloat32((number % 1000) / 10) as discount_rate,
  CAST(number % 2 = 0 AS Bool) as is_priority,
  toDate('2026-01-01') + toIntervalDay(number % 30) as sold_date,
  toDateTime64('2026-01-01 00:00:00', 3) + toIntervalSecond(number % 2592000) as sold_at
from numbers(1000000)
