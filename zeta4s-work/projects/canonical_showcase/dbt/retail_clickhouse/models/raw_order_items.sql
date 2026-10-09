{{ config(materialized='table') }}

select *
from values(
  'order_id UInt64, product_id UInt64, quantity UInt64, unit_price_cents UInt64',
  (1001, 501, 2, 1200),
  (1001, 503, 1, 1800),
  (1002, 502, 3, 700),
  (1003, 501, 1, 1200),
  (1004, 503, 2, 1800),
  (1005, 501, 1, 1250),
  (1005, 502, 1, 750)
)
