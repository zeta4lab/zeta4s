{{ config(materialized='table') }}

select 501 as product_id, 'COFFEE-250G' as sku, 'coffee' as category, 650 as unit_cost_cents from dual
union all select 502, 'TEA-20CT', 'tea', 300 from dual
union all select 503, 'MUG-12OZ', 'accessory', 450 from dual
