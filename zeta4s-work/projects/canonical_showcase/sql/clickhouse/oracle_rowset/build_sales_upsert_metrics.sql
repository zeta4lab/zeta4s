create table oracle_write_verify.sales_upsert_metrics
engine = MergeTree()
order by metric_name
as
select 'row_count' as metric_name, toFloat64(count()) as metric_value from oracle_write_verify.sales_upsert
union all
select 'updated_rows' as metric_name, toFloat64(countIf(sale_id between 100001 and 120000 and quantity > 10)) as metric_value from oracle_write_verify.sales_upsert
union all
select 'appended_rows' as metric_name, toFloat64(countIf(sale_id between 120001 and 140000)) as metric_value from oracle_write_verify.sales_upsert
union all
select 'priority_rows' as metric_name, toFloat64(countIf(sale_id between 100001 and 120000 and is_priority = 1)) as metric_value from oracle_write_verify.sales_upsert
