{{ config(materialized='table') }}

select *
from values(
  'product_id UInt64, sku String, category String, unit_cost_cents UInt64',
  (501, 'COFFEE-250G', 'coffee', 650),
  (502, 'TEA-20CT', 'tea', 300),
  (503, 'MUG-12OZ', 'accessory', 450)
)
