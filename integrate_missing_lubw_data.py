from login_credentials import *
from mqtt_controller.mqtt_controller import MQTTController
from lubw_controller.lubw_controller import LUBW_controller
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_time_ranges(start_time, end_time):
    """
    Generate a list of time ranges with 1-hour intervals between start_time and end_time.
    """
    start = datetime.fromisoformat(start_time)
    end = datetime.fromisoformat(end_time)
    while start < end:
        next_start = start + timedelta(hours=1)
        yield start.strftime('%Y-%m-%dT%H:%M:%S'), next_start.strftime('%Y-%m-%dT%H:%M:%S')
        start = next_start

if __name__ == "__main__":
    station = "DEBW015"
    mqtt_controller = MQTTController(station)
    lubw_controller = LUBW_controller()
    start_time = "2024-11-27T03:00:00"
    end_time = "2024-11-27T12:00:00"

    for current_start, current_end in generate_time_ranges(start_time, end_time):
        print(f"Fetching data from {current_start} to {current_end}...")

        get_lubw = LUBW_controller.fetch_station_data(station=station,
                                                      start_time=current_start,
                                                      end_time=current_end)

        if get_lubw is None or get_lubw.empty:
            print(f"No data available for range {current_start} to {current_end}")
            continue

        get_lubw['datetime_utc'] = pd.to_datetime(get_lubw['datetime']).dt.tz_convert('UTC').dt.strftime('%Y-%m-%dT%H:%M:%S')
        get_lubw['unix_time'] = (get_lubw['datetime'].astype(np.int64) // 10 ** 9).astype(int)
        timestamp = int(get_lubw["unix_time"].iloc[0])

        station_data = get_lubw.drop(columns=["datetime", "unix_time"]).to_dict(orient='records')

        latest_data = station_data[-1] if station_data else {}
        message = {
            "node_id": station,
            "timestamp": timestamp,
            "data": latest_data
        }
        mqtt_controller.publish_data(data=message)
    mqtt_controller.stop()
    pass