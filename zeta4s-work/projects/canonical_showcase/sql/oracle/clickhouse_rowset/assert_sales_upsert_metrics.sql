select
  case
    when (select count(*) from sales_upsert) = 140000
     and (select count(*) from sales_upsert_metrics) = 4
     and (select coalesce(max(metric_value), -1) from sales_upsert_metrics where metric_name = 'row_count') = 140000
     and (select coalesce(max(metric_value), -1) from sales_upsert_metrics where metric_name = 'updated_rows') = 20000
     and (select coalesce(max(metric_value), -1) from sales_upsert_metrics where metric_name = 'appended_rows') = 20000
     and (select coalesce(max(metric_value), -1) from sales_upsert_metrics where metric_name = 'priority_rows') = 20000
    then 1
    else 0
  end
from dual
