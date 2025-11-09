# stream_processor_carnot

Implementation Details-
`location_tracker.py`- Main service that reads the input stream, updates in‑memory state, and triggers periodic flush. 
 `query_service.py` - Utility to query  `output_stream.csv` for any device + time. 

Answers to Assignment Questions
**Q1- How will you read the csv in your service, given that the data points are only going to be
available at specified time (sts)?**
ans- i have used Pandas to read the csv file. sort the dataframe on the basis of sts, The smallest `sts` value is treated as the base time; subsequent events are delayed by their relative difference from the previous `sts`, ensuring each record becomes *visible only* at its intended arrival time.

**Q3- Using raw data excel as your stream, write the code for this service which stores the latest
data against every user at any given time. The output can be stored in another excel/csv
with time, user & his data details.**
ans- reference of the code can be find in location_tracker.py file.
  It:
    - Reads raw data  
    - Updates the `latest_locations` dictionary (per device)  
    - Flushes incremental updates every 1 second via a dedicated thread  
    - Ensures end‑to‑end latency < 2 seconds


**Q4- Write a function that takes in time & user as input, queries the above output excel/csv and
returns the latest data for given user at given time.**
ans- reference can be find in query_service.py file.
      It returns the latest valid record (≤ query time) from `output_stream.csv`.  
      All timestamps are handled in UTC for consistency.

**Q5- What is the average processing time for your service for the given raw data?**
ans-
<img width="555" height="236" alt="image" src="https://github.com/user-attachments/assets/530c6f11-b124-4b93-84b5-93008f2e2708" />
  - Average **record‑processing time:** ~ 0.04 ms  
  - Average **end‑to‑end latency (arrival → visible):** ~ 0.513 s


Q6- What do you think will be the bottlenecks in your service when the user base increases 1000
times? How will you handle those?
ans- when user base will increase 1000 times, than reading a core csv will only take too much time, for that we can shift to timeseries databases like influxDB or DynamoDB to store the data, with indexes. 
instead of single thread ingestion can introduce Sharded streams (bt device_pk_id). 
cant store big data in-memory (too much load on RAM)- can use caches like redis.



## How to run

```bash
pip install pandas

#  Run the stream processor
python location_tracker.py

# Query a specific user at a given time
python query_service.py
