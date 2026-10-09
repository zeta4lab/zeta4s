create table sales_upsert_metrics as
select 'row_count' as metric_name, cast(count(*) as number(18,0)) as metric_value from sales_upsert
union all
select 'updated_rows' as metric_name, cast(count(*) as number(18,0)) as metric_value
from sales_upsert
where sale_id between 100001 and 120000 and quantity > 10
union all
select 'appended_rows' as metric_name, cast(count(*) as number(18,0)) as metric_value
from sales_upsert
where sale_id between 120001 and 140000
union all
select 'priority_rows' as metric_name, cast(count(*) as number(18,0)) as metric_value
from sales_upsert
where sale_id between 100001 and 120000 and is_priority = 1
