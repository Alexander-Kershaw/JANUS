select 
    billing_date as date_day,
    plan_id,
    count(*) filter (where billing_event = 'start') as starts,
    count(distinct user_id) filter (where billing_event = 'start') as new_paid_users
from {{ ref('stg_billing')}}
group by 1, 2
order by 1, 2