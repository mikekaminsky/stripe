with events as (

  select * from {{ref('stripe_events')}}

)

select
  {{ nested_field('data', ['object', 'id']) }} as id,
  {{ nested_field('data', ['object', 'description']) }} as name,
  {{ nested_field('data', ['object', 'email']) }} as email,
  created as created_at,
  type as event_type
from events
where type in ('customer.deleted', 'customer.created', 'customer.updated')
