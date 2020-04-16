with events as (

    select * from {{ref('stripe_events')}}

)

select

    {{ nested_field('data', ['object', 'id']) }} as id,
    nullif({{ nested_field('data', ['object', 'subscription']) }}, '') as subscription_id,
    nullif({{ nested_field('data', ['object', 'charge']) }}, '') as charge_id,
    nullif({{ nested_field('data', ['object', 'customer']) }}, '') as customer_id,
    nullif(id, '') as event_id,

    type as event_type,

    {{ nested_field('data', ['object', 'date']) }} as invoice_date,
    {{ nested_field('data', ['object', 'period_end']) }} as period_end,
    {{ nested_field('data', ['object', 'period_start']) }} as period_start,

    {{ nested_field('data', ['object', 'currency']) }} as currency,
    {{ nested_field('data', ['object', 'attempt_count']) }} as attempt_count,
    {{ nested_field('data', ['object', 'attempted']) }} as attempted,
    {{ nested_field('data', ['object', 'closed']) }} as closed,

    {{ nested_field('data', ['object', 'total']) }} as total,
    {{ nested_field('data', ['object', 'subtotal']) }} as subtotal,

    {{ nested_field('data', ['object', 'amount_due']) }} as amount_due,
    {{ nested_field('data', ['object', 'next_payment_attempt']) }} as next_payment_attempt,
    {{ nested_field('data', ['object', 'paid']) }} as paid,
    {{ nested_field('data', ['object', 'forgiven']) }} as forgiven,

    created as created_at

from events
where type like 'invoice.%'
    and type not like '%payment%'
