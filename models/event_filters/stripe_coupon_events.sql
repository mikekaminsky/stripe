with events as (

  select * from {{ref('stripe_events')}}

)

select
  -- TODO: Deal with Epoch time
  created as created_at,
  {{ nested_field('data', ['object', 'currency']) }} as currency,
  {{ nested_field('data', ['object', 'id']) }} as id,
  {{ nested_field('data', ['object', 'percent_off']) }} as percent_discount,
  {{ nested_field('data', ['object', 'duration']) }} as duration,
  {{ nested_field('data', ['object', 'duration_in_months']) }} as duration_in_months,
  {{ nested_field('data', ['object', 'max_redemptions']) }} as max_redemptions,
  {{ nested_field('data', ['object', 'redeem_by']) }} as redeem_by,
  {{ nested_field('data', ['object', 'amount_off']) }} as amount_discount,
  {{ nested_field('data', ['object', 'valid']) }} as valid,
  type as event_type
from events
where type like 'coupon.%'
