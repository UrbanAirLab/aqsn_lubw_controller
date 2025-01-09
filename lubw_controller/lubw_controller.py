import requests
from requests.auth import AuthBase
import base64
import pandas as pd
from login_credentials import *

# Custom authentication class to handle UTF-8 encoding for the password
class UTF8BasicAuth(AuthBase):
    """Attaches HTTP Basic Authentication Header with UTF-8 encoding."""
    def __init__(self, lubw_username, lubw_password):
        self.username = lubw_username
        self.password = lubw_password

    def __call__(self, r):
        # Encode credentials with UTF-8
        auth_str = f'{self.username}:{self.password}'
        b64_encoded = base64.b64encode(auth_str.encode('utf-8')).decode('utf-8')
        r.headers['Authorization'] = f'Basic {b64_encoded}'
        return r

# Define components for each station
station_components = {
    "DEBW015": ['PM10', 'PM2.5', 'NO2', 'O3', 'TEMP', 'RLF', 'NSCH', 'STRG', 'WIV'],
    "DEBW152": ['NO2', 'CO']
}

class LUBW_controller:
    def __init__(self):
        self.username = lubw_username
        self.password = lubw_password
        self.base_url = lubw_base_url



    # Function to fetch data for a station and time range
    def fetch_station_data(station, start_time, end_time):

        def rename_columns(df, column_mapping):
            existing_columns = {old: new for old, new in column_mapping.items() if old in df.columns}
            df.rename(columns=existing_columns, inplace=True)
            return df

        # Get the components for the specified station
        components = station_components.get(station)

        if components is None:
            raise ValueError(f"Unknown station: {station}")

        all_data = {}  # Dictionary to store component data, keyed by datetime

        # Loop over components for the station
        for component in components:
            params = {
                'komponente': component,
                'von': start_time,
                'bis': end_time,
                'station': station
            }

            # Continue fetching data until no nextLink is provided
            next_link = None

            while True:
                try:
                    # If there's a next link, use it; otherwise, use the base URL with params
                    if next_link:
                        response = requests.get(next_link, auth=UTF8BasicAuth(lubw_username, lubw_password))
                    else:
                        response = requests.get(lubw_base_url, params=params, auth=UTF8BasicAuth(lubw_username, lubw_password))

                    response.raise_for_status()  # Raise an error for bad responses (4XX, 5XX)
                    response.encoding = 'utf-8'

                    data = response.json()

                    # Debugging: Print out the structure of the response for inspection
                    print(f"Component: {component} | Station: {station} | Data:")
                    print(data)  # Print the actual response

                    # Ensure the data is valid and contains 'messwerte'
                    if 'messwerte' not in data or not isinstance(data['messwerte'], list):
                        print(f"No 'messwerte' found for component {component} at station {station}")
                        break  # Stop if there's no data for this component

                    # Process each measurement (messwert)
                    for entry in data['messwerte']:
                        # Extract 'startZeit' as the datetime and 'wert' as the value
                        dt = entry['startZeit']  # Use 'startZeit' for datetime
                        value = entry['wert']  # Use 'wert' for the value

                        # Add datetime as a key, and create an empty dict for that datetime if not present
                        if dt not in all_data:
                            all_data[dt] = {'datetime': dt}

                        # Add the value for the current component to the dictionary
                        all_data[dt][component] = value

                    # Check for the nextLink to see if more data needs to be fetched
                    next_link = data.get('nextLink')

                    # Break if no more nextLink is present
                    if not next_link:
                        break

                except requests.exceptions.RequestException as e:
                    print(f"Error fetching data for component {component} at station {station}: {e}")
                    return None

        # Convert the dictionary into a DataFrame
        df = pd.DataFrame(list(all_data.values()))

        # Sort by datetime if needed
        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df.sort_values(by='datetime').reset_index(drop=True)
        column_mapping = {"PM10": "pm10", "PM2.5": "pm25", "TEMP": "sht_temp", "RLF": "sht_humid",
                          "NSCH": "sht_nsch", "STRG": "sht_strg", "WIV": "sht_wiv", "WIR":"sht_wir"}
        df = rename_columns(df, column_mapping)

        return df

