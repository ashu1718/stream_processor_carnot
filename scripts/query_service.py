# File: query_service.py
import pandas as pd
from datetime import datetime
def query_latest_at_time(device_id, query_time):
    """
    Query: What was the latest known location of `device_id` at `query_time`?
    """
    query_time = pd.to_datetime(query_time, utc=True)
    # print(query_time)
    log_df = pd.read_csv("../statics/output/output_stream.csv")
    log_df['time'] = pd.to_datetime(log_df['time'], utc=True)
    # print(log_df['time'])
    # print(log_df)
    # Filter by device and time
    valid_updates = log_df[
        (log_df['device_id'] == device_id) &
        (log_df['time'] <= query_time)
    ]

    # print(valid_updates)

    if valid_updates.empty:
        return None

    # Most recent before query time
    latest = valid_updates.sort_values('time').iloc[-1]
    return latest.to_dict()

# Example:
start_time= datetime.now()
result = query_latest_at_time(25029, '2021-10-23T12:32:45Z')
print("time taken to get latest data:" , datetime.now()-start_time)
print(result)