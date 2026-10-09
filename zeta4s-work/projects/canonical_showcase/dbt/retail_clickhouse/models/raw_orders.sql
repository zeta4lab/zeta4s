{{ config(materialized='table') }}

select *
from values(
  'order_id UInt64, customer_id UInt64, order_date Date, status String, channel String',
  (1001, 10, toDate('2026-01-01'), 'paid', 'web'),
  (1002, 11, toDate('2026-01-01'), 'paid', 'store'),
  (1003, 10, toDate('2026-01-02'), 'refunded', 'web'),
  (1004, 12, toDate('2026-01-02'), 'paid', 'marketplace'),
  (1005, 13, toDate('2026-01-03'), 'paid', 'web')
)
