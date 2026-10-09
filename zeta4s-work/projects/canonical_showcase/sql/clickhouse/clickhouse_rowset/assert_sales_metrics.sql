select
  (select count() from source.sales) = 1000000
  and (select count() from mart.stg_sales) = 1000000
  and (select count() from mart.sales_metrics) = 10000
  and (select sum(sale_count) from mart.sales_metrics) = 1000000
  and (select sum(total_quantity) from mart.sales_metrics) = 3000000
