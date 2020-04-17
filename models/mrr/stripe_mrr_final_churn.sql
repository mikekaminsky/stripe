{{ config(
  materialized = "table",
  dist = "id",
  sort = "date_month"
) }}

with mrr as (

    select * from {{ref('stripe_mrr_xf')}}

),

thin_air as (

    select
        date({{ dbt_utils.dateadd('MONTH', 1,'date_month') }}) as date_month,
        customer_id,
        TIMESTAMP({{ dbt_utils.dateadd('MONTH', 1,'rev_rec_date') }}) as rev_rec_date,
        plan_id,
        CAST(null AS timestamp) as period_start,
        CAST(null AS timestamp) as period_end,
        0 as mrr,
        0 as subscription_amount,
        0 as accrual_amount,
        0 as addon_amount,
        plan_name,
        plan_mrr_amount,
        plan_interval,
        0 as active_customer,
        0 as first_month,
        0 as last_month


    from mrr

    where last_month = 1

),

final as (

    select

        md5(CAST(date_month AS string) || customer_id) as id,
        *

    from thin_air

)

select * from final
