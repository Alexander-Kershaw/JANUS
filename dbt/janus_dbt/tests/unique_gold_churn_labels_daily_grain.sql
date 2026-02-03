select
  date_day,
  user_id,
  count(*) as n
from {{ ref('gold_churn_labels_daily') }}
group by 1, 2
having count(*) > 1