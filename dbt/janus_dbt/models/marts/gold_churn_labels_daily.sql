with base as (
    select
        date_day,
        user_id,
        is_active
    from {{ ref('fct_subscriptions_daily') }}
),

future as (
    select
        b.date_day,
        b.user_id,
        b.is_active as is_active_today,
        bool_or(f.is_active) as active_in_next_7d
    from base b
    left join base f
      on f.user_id = b.user_id
     and f.date_day > b.date_day
     and f.date_day <= b.date_day + interval '7 days'
    group by 1,2,3
)

select
    date_day,
    user_id,
    case
        when is_active_today
         and not coalesce(active_in_next_7d, false)
        then 1
        else 0
    end as churn_7d
from future