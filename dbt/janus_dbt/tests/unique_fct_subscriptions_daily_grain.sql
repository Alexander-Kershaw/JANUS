select
  date_day,
  user_id,
  count(*) as n
from {{ ref('fct_subscriptions_daily') }}
group by 1, 2
having count(*) > 1