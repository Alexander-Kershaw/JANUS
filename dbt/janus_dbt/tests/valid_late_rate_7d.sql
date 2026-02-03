select *
from {{ ref('gold_user_features_daily') }}
where late_rate_7d < 0 or late_rate_7d > 1