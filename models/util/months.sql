with dates as (

    select * from {{ref('days')}}

), final as (

    select distinct
        {{ dbt_utils.date_trunc('month', 'date_day') }} as date_month
    from dates

)

select * from final
