import uuid
from datetime import datetime, timezone

run_id = str(uuid.uuid4())
start_time = datetime.now(timezone.utc)


records_read = df.count()
records_good = good_df.count()
records_bad = bad_df.count()

if records_good > 0 and records_bad == 0:
    return "SUCCESS"
elif records_good > 0 and records_bad > 0:
    return "PARTIAL"
else:
    return "FAILED"
