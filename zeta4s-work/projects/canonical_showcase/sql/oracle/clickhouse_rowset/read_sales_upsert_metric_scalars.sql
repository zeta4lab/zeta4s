select
  max(case when metric_name = 'row_count' then metric_value end) as row_count,
  case
    when max(case when metric_name = 'row_count' then metric_value end) = 140000
     and max(case when metric_name = 'updated_rows' then metric_value end) = 20000
     and max(case when metric_name = 'appended_rows' then metric_value end) = 20000
     and max(case when metric_name = 'priority_rows' then metric_value end) = 20000
    then 1
    else 0
  end as ready
from sales_upsert_metrics
