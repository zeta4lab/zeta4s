{{ config(materialized='table') }}

select 1001 as order_id, 501 as product_id, 2 as quantity, 1200 as unit_price_cents from dual
union all select 1001, 503, 1, 1800 from dual
union all select 1002, 502, 3, 700 from dual
union all select 1003, 501, 1, 1200 from dual
union all select 1004, 503, 2, 1800 from dual
union all select 1005, 501, 1, 1250 from dual
union all select 1005, 502, 1, 750 from dual
