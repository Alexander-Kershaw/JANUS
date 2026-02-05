with base as (

    select
        date_day,
        plan_id,
        churn_7d
    from {{ ref('gold_user_features_daily') }}
    where plan_id is not null

),

agg as (

    select
        date_day,
        plan_id,
        count(*)                      as users,
        sum(churn_7d)                 as churners,
        avg(churn_7d)::float          as churn_rate
    from base
    group by 1, 2

)

select
    date_day,
    plan_id,
    users,
    churners,
    round((100 * churn_rate)::numeric, 3) as churn_rate_pct
from agg
order by date_day, plan_id
