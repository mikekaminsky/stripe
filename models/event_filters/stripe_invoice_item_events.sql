with events as (

    select * from {{ref('stripe_events')}}

)

select

    {{ nested_field('data', ['object', 'id']) }} as id,
    nullif({{ nested_field('data', ['object', 'invoice']) }}, '') as invoice_id,
    nullif({{ nested_field('data', ['object', 'customer']) }}, '') as customer_id,
    nullif({{ nested_field('data', ['object', 'subscription']) }}, '') as subscription_id,
    nullif(id, '') as event_id,

    type as event_type,

    timestamp_seconds({{ nested_field('data', ['object', 'date']) }}) as invoice_date,
    timestamp_seconds({{ nested_field('data', ['object', 'period', 'start']) }}) as period_start,
    timestamp_seconds({{ nested_field('data', ['object', 'period', 'end']) }}) as period_end,

    {{ nested_field('data', ['object', 'proration']) }} as proration,
    {{ nested_field('data', ['object', 'plan', 'id']) }} as plan_id,

    {{ nested_field('data', ['object', 'amount']) }} as amount,
    {{ nested_field('data', ['object', 'currency']) }} as currency,

    {{ nested_field('data', ['object', 'description']) }} as description,

    created as created_at

from events
where type like 'invoiceitem.%'
