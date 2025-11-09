import pandas as pd
import time
import threading
from datetime import datetime
from pathlib import Path
import statistics
# ------------------------------------------------------------
# Config
# ------------------------------------------------------------
INPUT_FILE = "../statics/input/raw_data.csv"
OUTPUT_FILE = "../statics/output/output_stream.csv"
FLUSH_INTERVAL = 1.0
MAX_REAL_SLEEP = 2.0  # never wait longer than this

# ------------------------------------------------------------
# Shared state
# ------------------------------------------------------------
latest_locations = {}
dirty_devices = set()
lock = threading.Lock()
shutdown = threading.Event()

output_columns = ['time', 'device_id', 'lat', 'lon', 'speed', 'event_time']
Path(OUTPUT_FILE).write_text(','.join(output_columns) + '\n')

latency_records=[]
# ------------------------------------------------------------
# Flusher Thread
# ------------------------------------------------------------

def periodic_flush():
    """Flush only devices that changed since last flush."""
    while not shutdown.is_set():
        time.sleep(FLUSH_INTERVAL)

        # snapshot & clear dirty set
        with lock:
            if not dirty_devices:
                continue
            to_write = list(dirty_devices)       # copy ids
            dirty_devices.clear()
            rows=[]
            flush_time= time.time()
            for dev in to_write:
                if dev in latest_locations:
                    rows.append({
                        'time': latest_locations[dev]['updated_at'],
                        'device_id': dev,
                        'lat': latest_locations[dev]['lat'],
                        'lon': latest_locations[dev]['lon'],
                        'speed': latest_locations[dev]['speed'],
                        'event_time': latest_locations[dev]['time_stamp']
                    })
                latency_records.append((
                    latest_locations[dev]['entering_time'],
                    flush_time
                ))
            
        if rows:
            pd.DataFrame(rows).to_csv(OUTPUT_FILE, mode='a', header=False, index=False)
            print(f"🟢 Flushed {len(rows)} updated devices at {datetime.now().time()}")

# ------------------------------------------------------------
# Stream Processor
# ------------------------------------------------------------
def simulate_stream():
    df = pd.read_csv(INPUT_FILE)
    
    df['sts'] = pd.to_datetime(df['sts'], utc=True)
    df['time_stamp'] = pd.to_datetime(df['time_stamp'], utc=True)
    df = df.sort_values('sts').reset_index(drop=True)

    prev_sts = None
    processing_times=[]
    print("🚀 Starting optimized threaded simulation...")
    for _, row in df.iterrows():
        current_sts = row['sts']
        # incremental, capped wait
        if prev_sts is not None:
            delta_sec = (current_sts - prev_sts).total_seconds()
            time.sleep(min(delta_sec, MAX_REAL_SLEEP))
        prev_sts = current_sts

        start_time_for_each_process= time.time()
        device_id = row['device_fk_id']
        event_time = row['time_stamp']
        with lock:
            cur = latest_locations.get(device_id)
            if cur is None or event_time > cur['time_stamp']:
                latest_locations[device_id] = {
                    'lat': row['latitude'],
                    'lon': row['longitude'],
                    'speed': row['speed'],
                    'time_stamp': event_time,
                    'updated_at': current_sts,
                    'entering_time': time.time()
                }
                
                dirty_devices.add(device_id)
        end_time_for_each_process= time.time()
        processing_times.append(end_time_for_each_process- start_time_for_each_process)
    print("✅ Finished ingesting all events.")
    shutdown.set()  # tell flusher to stop
    avg_time= statistics.mean(processing_times)
    print(f"Average per-record processing time: {avg_time*1000:.2f} ms")
# ------------------------------------------------------------
# Main entrypoint
# ------------------------------------------------------------
if __name__ == "__main__":
    flusher_thread = threading.Thread(target=periodic_flush, daemon=True)
    flusher_thread.start()

    simulate_stream()

    # give the flusher one last chance to write remaining updates
    flusher_thread.join(timeout=FLUSH_INTERVAL + 0.5)
    print("✅ All done. Output file:", OUTPUT_FILE)

    latencies= [flush - arrival for arrival, flush in latency_records]
    if latencies:
        print(f"Average end‑to‑end latency: {statistics.mean(latencies):.3f} s")