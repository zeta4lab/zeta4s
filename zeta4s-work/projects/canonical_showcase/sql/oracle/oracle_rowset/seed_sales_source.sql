create table sales as
select
  cast(sale_id as number(19)) as sale_id,
  cast(mod(sale_id - 1, 50) + 1 as number(5)) as store_id,
  cast(mod(sale_id - 1, 10000) + 1 as number(10)) as product_id,
  'customer-' || to_char(mod(sale_id - 1, 250000) + 1) as customer_id,
  cast(mod(sale_id - 1, 5) + 1 as number(3)) as quantity,
  cast(mod(sale_id - 1, 20000) + 100 as number(10)) as unit_price_cents,
  cast((mod(sale_id - 1, 5) + 1) * (mod(sale_id - 1, 20000) + 100) as number(18)) as amount_cents,
  cast(((mod(sale_id - 1, 5) + 1) * (mod(sale_id - 1, 20000) + 100)) / 100 as number(18,2)) as amount,
  cast(mod(sale_id - 1, 1000) / 10 as binary_double) as discount_rate,
  cast(case when mod(sale_id - 1, 2) = 0 then 1 else 0 end as number(1)) as is_priority,
  cast(date '2026-01-01' + mod(sale_id - 1, 30) as date) as sold_date,
  cast(timestamp '2026-01-01 00:00:00' + numtodsinterval(mod(sale_id - 1, 2592000), 'SECOND') as timestamp) as sold_at
from (
  select ((block_gen.block_id - 1) * 1000) + row_gen.row_id as sale_id
  from (select level as block_id from dual connect by level <= 5000) block_gen
  cross join (select level as row_id from dual connect by level <= 1000) row_gen
)
