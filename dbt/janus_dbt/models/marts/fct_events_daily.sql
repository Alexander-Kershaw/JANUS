with base as (
    select
        date_trunc('day', event_ts)::date as date_day,
        user_id,
        is_late
    from {{ ref('stg_events')}}
)

select 
    date_day,
    count(*) as events,
    count(distinct user_id) filter (where user_id is not null) as dau,
    count(*) filter (where is_late) as late_events
from base
group by 1
order by 1

