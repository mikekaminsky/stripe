{{
    config(
        materialized = 'table',
        sort = 'transaction_date',
        dist = 'customer_id'
    )
}}


with unioned as (

    select * from {{ref('stripe_addon_transactions')}}

    union all

    select * from {{ref('stripe_subscription_transactions_amortized')}}

),

final as (

    select
        date({{ dbt_utils.date_trunc('month', 'transaction_date') }}) as date_month,
        source_item_type,
        source_item_id,
        subscription_id,
        customer_id,
        transaction_date,
        invoice_date,
        period_start,
        period_end,
        CAST(amount AS numeric) / 100 as amount,
        plan_id,
        forgiven,
        paid,
        duration

    from unioned

)

select * from final
