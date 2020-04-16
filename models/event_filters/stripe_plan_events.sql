with events as (

  select * from {{ref('stripe_events')}}

)

select
  {{ nested_field('data', ['object', 'id']) }} as id,
  {{ nested_field('data', ['object', 'amount']) }} as amount,
  {{ nested_field('data', ['object', 'currency']) }} as currency,
  {{ nested_field('data', ['object', 'name']) }} as name,
  type as event_type,
  {{ nested_field('data', ['object', 'interval']) }} as plan_interval,
  created as created_at
from events
where type like 'plan%'
