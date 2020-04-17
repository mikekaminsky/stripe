with events as (

    select * from {{ref('stripe_events')}}

),

final as (

    select

        {{ nested_field('data', ['object', 'id']) }} as id,
        id as event_id,
        {{ nested_field('data', ['object', 'customer']) }} as customer_id,

        created as created_at,

        {{ nested_field('data', ['object', 'status']) }} as status,
        type as event_type,

        {{ nested_field('data', ['object', 'start']) }} as start,
        timestamp_seconds({{ nested_field('data', ['object', 'current_period_start']) }}) as period_start,
        timestamp_seconds({{ nested_field('data', ['object', 'current_period_end']) }}) as period_end,
        {{ nested_field('data', ['object', 'canceled_at']) }} as canceled_at,

        {{ nested_field('data', ['object', 'quantity']) }} as quantity,

        {{ nested_field('data', ['object', 'plan', 'id']) }} as plan_id,
        {{ nested_field('data', ['object', 'plan', 'interval']) }} as plan_interval,
        {{ nested_field('data', ['object', 'plan', 'amount']) }} as plan_amount

    from events

    where type like 'customer.subscription.%'

)

select * from final
