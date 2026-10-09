{{ config(materialized='table') }}

select
  order_date,
  channel,
  count(*) as order_count,
  sum(gross_revenue_cents) as gross_revenue_cents,
  sum(margin_cents) as margin_cents
from {{ ref('order_revenue') }}
group by order_date, channel
