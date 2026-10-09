{{ config(materialized='table') }}

select
  orders.order_id as order_id,
  orders.customer_id as customer_id,
  orders.order_date as order_date,
  orders.channel as channel,
  sum(items.quantity * items.unit_price_cents) as gross_revenue_cents,
  sum(items.quantity * products.unit_cost_cents) as cost_cents,
  sum(items.quantity * items.unit_price_cents) - sum(items.quantity * products.unit_cost_cents) as margin_cents
from {{ ref('raw_orders') }} as orders
join {{ ref('raw_order_items') }} as items
  on orders.order_id = items.order_id
join {{ ref('raw_products') }} as products
  on items.product_id = products.product_id
where orders.status = 'paid'
group by
  orders.order_id,
  orders.customer_id,
  orders.order_date,
  orders.channel
