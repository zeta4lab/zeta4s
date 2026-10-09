{{ config(materialized='table') }}

select 1001 as order_id, 10 as customer_id, DATE '2026-01-01' as order_date, 'paid' as status, 'web' as channel from dual
union all select 1002, 11, DATE '2026-01-01', 'paid', 'store' from dual
union all select 1003, 10, DATE '2026-01-02', 'refunded', 'web' from dual
union all select 1004, 12, DATE '2026-01-02', 'paid', 'marketplace' from dual
union all select 1005, 13, DATE '2026-01-03', 'paid', 'web' from dual
