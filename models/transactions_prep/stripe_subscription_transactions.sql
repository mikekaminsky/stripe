with incremented as (

    select * from {{ref('stripe_subscription_transactions_incremented')}}

),

final as (

    select

        source_item_type,
        source_item_id,
        subscription_id,
        customer_id,
        invoice_date,
        period_start,

        case
            when duration = 'year'
                then period_end

            when max_period_start <= extract(day from period_end) then period_end

            else

                least(

                    timestamp(date(
                        extract(year from period_end),
                        extract(month from period_end),
                        extract(day from {{ dbt_utils.last_day('period_end', 'month') }})
                    )),

                    following_period_start

                )

        end as period_end,

        amount,
        plan_id,
        forgiven,
        paid,
        duration

    from incremented

)

select * from final
