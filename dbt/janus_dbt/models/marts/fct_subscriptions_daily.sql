with spine as (
    select generate_series(
        (select min(billing_date) from {{ ref('stg_billing') }}),
        (select max(billing_date) from {{ ref('stg_billing') }}),
        interval '1 day'
    )::date as date_day
),

events as (
    select
        billing_date as date_day,
        user_id,
        billing_event,
        plan_id
    from {{ ref('stg_billing') }}
),

state as (
    select
        s.date_day,
        u.user_id,
        (
            select e.plan_id
            from events e
            where e.user_id = u.user_id
              and e.date_day <= s.date_day
              and e.billing_event in ('start','upgrade')
            order by e.date_day desc
            limit 1
        ) as last_plan,
        (
            select e.date_day
            from events e
            where e.user_id = u.user_id
              and e.date_day <= s.date_day
              and e.billing_event = 'cancel'
            order by e.date_day desc
            limit 1
        ) as last_cancel_day,
        (
            select e.date_day
            from events e
            where e.user_id = u.user_id
              and e.date_day <= s.date_day
              and e.billing_event in ('start','upgrade')
            order by e.date_day desc
            limit 1
        ) as last_start_or_upgrade_day
    from spine s
    cross join (select distinct user_id from events) u
)

select
    date_day,
    user_id,
    case
        when last_start_or_upgrade_day is null then false
        when last_cancel_day is not null and last_cancel_day >= last_start_or_upgrade_day then false
        else true
    end as is_active,
    case
        when last_start_or_upgrade_day is null then null
        when last_cancel_day is not null and last_cancel_day >= last_start_or_upgrade_day then null
        else last_plan
    end as plan_id
from state
order by date_day, user_id