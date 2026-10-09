select
  metric_rows,
  sale_count,
  if(metric_rows = 10000 and sale_count = 1000000, 1, 0) as ready
from
(
  select
    count() as metric_rows,
    sum(sale_count) as sale_count
  from mart.sales_metrics
)
