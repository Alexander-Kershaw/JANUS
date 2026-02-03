select
  billing_date,
  user_id,
  event as billing_event,
  plan_id,
  source_file,
  ingestion_ts
from {{ source('silver', 'silver_billing') }}