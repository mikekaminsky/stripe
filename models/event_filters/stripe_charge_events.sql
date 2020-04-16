with charges as (

  select * from {{ref('stripe_events')}}

)

select
  id as event_id,
  {{ nested_field('data', ['object', 'id']) }} as charge_id,
  {{ nested_field('data', ['object', 'customer']) }} as customer_id,
  {{ nested_field('data', ['object', 'invoice']) }} as invoice_id,
  {{ nested_field('data', ['object', 'balance_transaction']) }} as balance_transaction_id,
  {{ nested_field('data', ['object', 'amount']) }} as amount,
  replace(type, 'charge.', '') as result,
  {{ nested_field('data', ['object', 'created']) }} as created_at
from charges
where type like 'charge.%'
