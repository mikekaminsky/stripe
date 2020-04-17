{{ config(
  materialized = "table",
  dist = "customer_id",
  sort = "transaction_date"
) }}

--this model amortizes values first into a daily output then summarizes by month

with transactions as (

    select * from {{ref('stripe_subscription_transactions_unioned')}}

),

days as (

    select * from {{ref('days')}}

),

amortized as (

    select

        transactions.*,

        TIMESTAMP({{ dbt_utils.dateadd(
            'day', dbt_utils.datediff(
              dbt_utils.date_trunc('day', 'transactions.period_start'),
            "date_day", 'day'),
          'invoice_date') }}) as transaction_date

    from transactions

    inner join days

        on date({{ dbt_utils.date_trunc('day', 'transactions.period_start') }}) <= days.date_day
        and date({{ dbt_utils.date_trunc('day', 'transactions.period_end') }}) > days.date_day

),

month_days_tbl as (

    select
        *,
        extract(day from {{ dbt_utils.last_day('transaction_date', 'month') }}) as month_days,
        extract(day from transaction_date) as transaction_day,
        extract(day from invoice_date) as invoice_day,
        {{ dbt_utils.date_trunc('month', 'transaction_date') }} as date_month
    from amortized

),

calculated as (

--the below fields calculated daily values for annual contracts to properly be
--able to sum up the amounts monthly regardless of the number of days in a month

    select
        *,
        1/month_days/12 as daily_calculated_proportion,
        (1/month_days/12)*amount as daily_calculated_amount,
        (1/month_days/12) * amount * month_days as calculated_mrr,
        case
            when max(transaction_day) over (partition by customer_id, date_month
            order by transaction_date rows between unbounded preceding and
            unbounded following) = transaction_day
                then 1
            else null
        end as customer_last_month_value,
        case
            when invoice_day = transaction_day
                then 1
            when invoice_day > month_days
            and transaction_day = month_days
                then 1
            else null
        end as rev_rec_date_base,
        case
            when max(date_month) over (partition by customer_id order by
            date_month rows between unbounded preceding and unbounded following)
            = date_month
                then 1
            else null
        end as last_month
    from month_days_tbl

),

final as (

    select
        *,
        case
            when last_month = 1
                then 0
            when duration = 'month'
                then amount
            else calculated_mrr
        end as mrr_amount
    from calculated

)

select * from final
