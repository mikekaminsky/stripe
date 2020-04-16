with items as (

    select * from {{ref('stripe_invoice_items')}}

),

invoices as (

    select * from {{ref('stripe_invoices')}}

),

final as (

    select

        cast('proration item' as string) as source_item_type,
        items.id as source_item_id,
        items.subscription_id,
        items.customer_id,
        items.invoice_date,
        case
            when invoices.subscription_id is null
            and {{ dbt_utils.datediff("items.period_start", "items.period_end", 'month') }}
                then items.period_end
            else items.period_start
        end as period_start,
        items.period_end,
        items.amount,
        items.plan_id,
        invoices.forgiven,
        invoices.paid,
        case
            when  {{ dbt_utils.datediff("items.period_start", "items.period_end", 'month') }} > 1
                then 'year'
            else 'month'
        end as duration

    from items

    inner join invoices on items.invoice_id = invoices.id

    where items.proration = true
        and items.deleted_at is null

)

select * from final
