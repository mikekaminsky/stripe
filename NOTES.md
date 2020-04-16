# Notes

* What's the database-agnostic version of this?
```
, spline AS (
  {{ dbt_utils.date_spine(
      start_date="DATETIME(2008-01-01)",
      datepart="day",
      end_date="DATETIME(CURRENT_DATE)"
     )
  }}
)
```

* Is this change cross-DB valid?
```
amount::float / 12
CAST(amount AS float) / 12
```

* How to deal with this annoying issue
```
  where "type" like 'coupon.%'
  where type like 'coupon.%'
```

* timestamps all appear to be deliminted in seconds since unix epoc. [See here](https://stripe.com/docs/api/orders/list#list_orders-created)
  * That does not seem to match the code currently in this db so I'm not sure how this was handled before :shrug:
