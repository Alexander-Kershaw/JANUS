with
max_date as (
    select max(event_ts::date) as max_date
    from {{ ref('stg_events') }}
),
days as (
    select distinct date_day
    from {{ ref('fct_subscriptions_daily') }}
),

events as (
    select
      event_ts::date as event_day,
      user_id,
      session_id,
      event_type,
      is_late
    from {{ ref('stg_events') }}
    where user_id is not null
),

users as (
    select distinct user_id
    from {{ ref('fct_subscriptions_daily') }}
),

agg as (
    select
      d.date_day,
      u.user_id,

      count(*) filter (
        where e.event_day > d.date_day - 7
          and e.event_day <= d.date_day
      ) as events_7d,

      count(distinct e.session_id) filter (
        where e.event_day > d.date_day - 7
          and e.event_day <= d.date_day 
      ) as sessions_7d,

      count(*) filter (
        where e.event_type = 'feature_use'
          and e.event_day > d.date_day - 7
          and e.event_day <= d.date_day
      ) as feature_use_7d,

      count(*) filter (
        where e.event_type = 'support_ticket'
          and e.event_day > d.date_day - 14
          and e.event_day <= d.date_day
      ) as support_tickets_14d,

      case
        when count(*) filter (
          where e.event_day > d.date_day - 7
            and e.event_day <= d.date_day
        ) = 0 then 0.0
        else
          count(*) filter (
            where e.is_late
              and e.event_day > d.date_day - 7
              and e.event_day <= d.date_day
          )::float
          /
          count(*) filter (
            where e.event_day > d.date_day - 7
              and e.event_day <= d.date_day
          )::float
      end as late_rate_7d

    from days d
    join users u on true
    left join events e
      on e.user_id = u.user_id
    group by 1, 2
),

subs as (
    select date_day, user_id, is_active, plan_id
    from {{ ref('fct_subscriptions_daily') }}
),

labels as (
    select date_day, user_id, churn_7d
    from {{ ref('gold_churn_labels_daily') }}
)
select
  a.date_day,
  a.user_id,
  s.is_active,
  s.plan_id,
  a.events_7d,
  a.sessions_7d,
  a.feature_use_7d,
  a.support_tickets_14d,
  a.late_rate_7d,
  l.churn_7d
from agg a
left join subs s
  on s.date_day = a.date_day
 and s.user_id = a.user_id
left join labels l
  on l.date_day = a.date_day
 and l.user_id = a.user_id
where a.date_day <= (select max(date_day) from {{ ref('fct_subscriptions_daily') }}) - interval '7 days'
