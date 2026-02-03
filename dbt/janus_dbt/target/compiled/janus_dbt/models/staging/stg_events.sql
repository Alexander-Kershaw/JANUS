select
  event_id,
  event_ts,
  received_ts,
  user_id,
  device_id,
  session_id,
  event_type,
  props,
  is_late,
  lateness_sec,
  source_file,
  ingestion_ts
from silver.silver_events