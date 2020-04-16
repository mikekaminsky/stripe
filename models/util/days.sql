with

events as ( select * from {{ref('stripe_events')}})

, start_day AS (
  SELECT MIN(DATE(created)) AS first_date
  FROM events
)

, spline AS (
  {{ dbt_utils.date_spine(
      start_date="DATETIME('2008-01-01')",
      datepart="day",
      end_date="DATETIME(CURRENT_DATE)"
     )
  }}
)

SELECT
  DATE(spline.date_day) AS date_day
FROM spline
CROSS JOIN start_day
WHERE DATE(date_day) >= first_date
