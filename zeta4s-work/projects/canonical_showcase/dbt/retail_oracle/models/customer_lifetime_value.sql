{{ config(materialized='table') }}

select
  customer_id,
  count(*) as paid_order_count,
  sum(gross_revenue_cents) as lifetime_revenue_cents,
  sum(margin_cents) as lifetime_margin_cents
from {{ ref('order_revenue') }}
group by customer_id
