# extract_locations → extract_sensors → extract_measurements → validate_measurements → load_to_postgres.
from openaq import OpenAQ
from openaq.core.exceptions import RateLimitError
import time

import os
import pandas as pd
import logging
from datetime import date, timedelta, datetime


OPENAQ_API_KEY = "b8b5e23b35f930e0252e8ba7668c19ebe5cf82ff8fc6b8e73ead8de9e7d6a1ca"

csv_file = 'temp_measurements.csv'

PAGE = 1
LIMIT = 1000
COUNTRY_ID = 17 #Kenya

NAIROBI_COORDINATES = {
    "lat": -1.286389,
    'lon': 36.817223,
    'radius': 25000 #25KM
}



def extract_with_coordinates(lat, lon, radius, page=1):
    try:
        with OpenAQ(api_key=OPENAQ_API_KEY) as client:
            coordinates = (lat, lon)
            location = client.locations.list(coordinates=coordinates, radius=radius, page=page, limit=LIMIT)
            
            results = location.results

            print(f"Retrieved {len(results)} entries")
            print(f"Current page: {page}")

            return location.dict()
    except Exception as e:
        print(e)

def extract_headers(data):
    return data['headers']

def extract_meta(data):
    return data['meta']

def extract_result(data):
    return data['results']

def extract_locations(data):
    locations = []

    for entry in data:
        locations.append(
            (entry['id'], entry['name'])
        )
    return locations


def extract_sensors_from_location(location):
    try:
        with OpenAQ(api_key=OPENAQ_API_KEY) as client:
            location_id = location[0]

            sensors = client.locations.sensors(location_id)
            sensors = sensors.dict()['results']
            print(sensors)
            return sensors
    except Exception as e:
        print(e)


def extract_measurements_from_sensor_paginated(
    client, sensor_id, location_id, location_name, date_from, date_to
):
    # result = []
    resp = []
    page = PAGE

    while True:
        try:
            measurements = client.measurements.list(
                sensors_id=sensor_id,

                # data="days",
                # date_from=date_from,
                # date_to=date_to,

                data="measurements",
                datetime_from=date_from,
                datetime_to=date_to,

                page=page,
                limit=LIMIT,
            )
        except RateLimitError:
            print("Rate limit reached; waiting 60 seconds before retrying")
            time.sleep(60)
            continue

        measurements_results = measurements.dict()["results"]

        if not measurements_results:
            print(f"No results on page {page}. Exiting ...")
            break

        # result.extend(measurements_results)
        page += 1

        ####################
        rows = []
        for measurement in measurements_results:
            rows.append({
                "location_id": location_id,
                "location_name": location_name,
                "sensor_id": sensor_id,
                "parameter": measurement["parameter"]["name"],
                "unit": measurement["parameter"]["units"],
                "value": measurement["value"],
                "measurement_timestamp": measurement["period"]["datetime_from"]["utc"],
            })
        resp.extend(rows)

        df = pd.DataFrame(rows)
        df.to_csv(csv_file, mode="a", header=not os.path.exists(csv_file), index=False)

        ####################

    # return result
    return resp




def main():
    logger = logging.getLogger("openaq")
    logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    logger.addHandler(handler)
    print('\n\n')
    
    
    if os.path.exists(csv_file):
        os.remove(csv_file)

    try:
        unextraceted_data = extract_with_coordinates(lat=NAIROBI_COORDINATES['lat'], lon=NAIROBI_COORDINATES['lon'], radius=NAIROBI_COORDINATES['radius'], page=PAGE)
        extracted_data = extract_result(unextraceted_data)
        locations = extract_locations(extracted_data)

        sensors_list = []
        for location in locations:
            sensors = extract_sensors_from_location(location)
            location_id = location[0]
            location_name = location[1]
            # print(sensors)
            for sensor in sensors:
                sensors_list.append([sensor['id'], location_id, location_name])


        date_from="2026-08-01"
        date_to="2026-09-25"
        date_format = "%Y-%m-%d"


        # date_to = datetime.strptime(date_to, date_format).date()
        # date_from = datetime.strptime(date_from, date_format).date()

        date_to = datetime.strptime(date_to, date_format)
        date_from = datetime.strptime(date_from, date_format)


        with OpenAQ(api_key=OPENAQ_API_KEY, auto_wait=True) as client:
            measurements_list = []

            for sensor_entry in sensors_list:
                sensor_id = sensor_entry[0]
                location_id = sensor_entry[1]
                location_name = sensor_entry[2]
                measurements = extract_measurements_from_sensor_paginated(
                    location_id=location_id,
                    location_name=location_name,

                    client=client,
                    sensor_id=sensor_id,
                    date_from=date_from,
                    date_to=date_to,
                )
                measurements_list.extend(measurements)
            print(f'''
                \n
                measurements-list\n
            --------------\n
            {measurements_list}\n
            ''')



    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()