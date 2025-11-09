import pandas as pd
import time
import threading
from datetime import datetime
from pathlib import Path
import statistics

#define the input output directories
INPUT_FILE = "../statics/input/raw_data.csv"
OUTPUT_FILE = "../statics/output/output_stream.csv"
FLUSH_INTERVAL = 1.0
MAX_REAL_SLEEP = 2.0  # never wait longer than this


latest_locations = {} #shared obj for per device latest data storage
dirty_devices = set() # a set containing id of changed devices
lock = threading.Lock() # to avoid dirty read and write
shutdown = threading.Event()

output_columns = ['time', 'device_id', 'lat', 'lon', 'speed', 'event_time']
Path(OUTPUT_FILE).write_text(','.join(output_columns) + '\n')

latency_records=[] #stores start and end time for end-to-end latency calc

# flush data at regular time instead of writing on each update to avoid too much i/o op
def periodic_flush():
    """Flush only devices into output file that changed since last flush."""
    while not shutdown.is_set():
        time.sleep(FLUSH_INTERVAL)

        
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
                # store arrival time and flush time for each record
                latency_records.append((
                    latest_locations[dev]['entering_time'],
                    flush_time
                ))
            
        if rows:
            pd.DataFrame(rows).to_csv(OUTPUT_FILE, mode='a', header=False, index=False)
            print(f" Flushed {len(rows)} updated devices at {datetime.now().time()}")



def simulate_stream():
    """
        function to read csv, process data and update the latest data per device.
    """
    df = pd.read_csv(INPUT_FILE)
    
    df['sts'] = pd.to_datetime(df['sts'], utc=True)
    df['time_stamp'] = pd.to_datetime(df['time_stamp'], utc=True)
    df = df.sort_values('sts').reset_index(drop=True)

    prev_sts = None
    processing_times=[]
    print(" Starting threaded execution...")
    for _, row in df.iterrows():
        current_sts = row['sts']
    
        if prev_sts is not None:
            delta_sec = (current_sts - prev_sts).total_seconds() #used diff of curr sts and prev sts to get arrival time
            time.sleep(min(delta_sec, MAX_REAL_SLEEP))
        prev_sts = current_sts

        start_time_for_each_process= time.time()
        device_id = row['device_fk_id']
        event_time = row['time_stamp']
        with lock:
            cur = latest_locations.get(device_id)
            if cur is None or event_time > cur['time_stamp']:
                #update latest data to shared obj
                latest_locations[device_id] = {
                    'lat': row['latitude'],
                    'lon': row['longitude'],
                    'speed': row['speed'],
                    'time_stamp': event_time,
                    'updated_at': current_sts,
                    'entering_time': time.time()
                }
                #add device id with latest update
                dirty_devices.add(device_id)
        end_time_for_each_process= time.time()

        # push each event's processing time. 
        processing_times.append(end_time_for_each_process- start_time_for_each_process)
    print(" Finished ingesting all events.")
    shutdown.set()  # tell flusher to stop

    avg_time= statistics.mean(processing_times)
    print(f"Average per-record processing time: {avg_time*1000:.2f} ms")


if __name__ == "__main__":
    '''
        use different thread for data flush into output file to avoid any delays.
    '''
    flusher_thread = threading.Thread(target=periodic_flush, daemon=True)
    flusher_thread.start()
    # run processing on main thread
    simulate_stream()

   
    flusher_thread.join(timeout=FLUSH_INTERVAL + 0.5)
    print("All done. Output file:", OUTPUT_FILE)

    # avg out all latencies stored during processing. 
    latencies= [flush - arrival for arrival, flush in latency_records]
    if latencies:
        print(f"Average end‑to‑end latency: {statistics.mean(latencies):.3f} s")