with events as (

    select * from {{ref('stripe_events')}}

),

final as (

    select

        {{ nested_field('data', ['object', 'id']) }} as id,
        created as created_at,
        type as event_type,
        {{ nested_field('data', ['object', 'customer']) }} as customer_id,
        {{ nested_field('data', ['object', 'coupon', 'id']) }} as coupon_id,

        case
            when {{ nested_field('data', ['object', 'coupon', 'percent_off']) }} is null then 'amount'
            else 'percent'
        end as discount_type,

        coalesce({{ nested_field('data', ['object', 'coupon', 'percent_off']) }},
                {{ nested_field('data', ['object', 'coupon', 'amount_off']) }}) as discount_value,

        {{ nested_field('data', ['object', 'start']) }} as discount_start,
        {{ nested_field('data', ['object', 'end']) }} as discount_end

    from events

    where type like 'customer.discount.%'

)

select * from final
