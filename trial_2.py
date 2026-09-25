# extract_locations → extract_sensors → extract_measurements → validate_measurements → load_to_postgres.
from openaq import OpenAQ
from openaq.core.exceptions import RateLimitError
import time

import os
import pandas as pd
import logging
from datetime import date, timedelta, datetime


OPENAQ_API_KEY = "b8b5e23b35f930e0252e8ba7668c19ebe5cf82ff8fc6b8e73ead8de9e7d6a1ca"
# INVALID_OPENAQ_API_KEY = "asjbakjvbsavjabvjkavbakjvbakjvbajvabvkjabv"

CSV_FILE = "load_file_openAQ.csv"

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

'''
def extract_sensors(data):
    return data['sensors']
'''

def extract_sensors_from_location(location):
    try:
        with OpenAQ(api_key=OPENAQ_API_KEY) as client:
            location_id = location[0]
            location_name = location[1]

            sensors = client.locations.sensors(location_id)
            sensors = sensors.dict()['results']
            print(sensors)
            # print(f"retrieved {len(sensors)} sensors for location with id: {location_id} and location name: {location_name}]")
            return sensors
    except Exception as e:
        print(e)

'''
def extract_measurements_from_all_sensors(sensors):
    with OpenAQ(api_key=OPENAQ_API_KEY) as client:
        try:
            result = []
            for sensor in sensors:
                measurements = client.measurements.list(sensor.get('id'), data='measurements')
                result.append(measurements['results'])
                
            return result

        except Exception as e:
            print(e)
'''


def extract_measurements_from_sensor(sensor):
    with OpenAQ(api_key=OPENAQ_API_KEY) as client:
        try:
            measurements = client.measurements.list(sensor.get('id'), data='measurements')
            return measurements['results']
        except Exception as e:
            print(e)

def extract_measurements_from_sensor_paginated(
    client, sensor_id, date_from, date_to
):
    result = []
    page = PAGE

    while True:
        try:
            measurements = client.measurements.list(
                sensors_id=sensor_id,
                data="days",
                date_from=date_from,
                date_to=date_to,
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

        result.extend(measurements_results)
        page += 1

    return result




def main():
    logger = logging.getLogger("openaq")
    logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    logger.addHandler(handler)
    print('\n\n')
    
    try:
        unextraceted_data = extract_with_coordinates(lat=NAIROBI_COORDINATES['lat'], lon=NAIROBI_COORDINATES['lon'], radius=NAIROBI_COORDINATES['radius'], page=PAGE)
        extracted_data = extract_result(unextraceted_data)
        locations = extract_locations(extracted_data)
        # save_to_csv('locations.csv', locations)
        print(f'''
              locations\n
              --------------\n
              {locations}\n
              ''')

        sensors_list = []
        for location in locations:
            sensors = extract_sensors_from_location(location)
            print(sensors)
            for sensor in sensors:
                sensors_list.append(sensor['id'])
            # save_to_csv('sensors.csv', sensors)
            # sensors_list.append(sensors['id'])
        print(f'''
            \n
            sensors-list\n
          --------------\n
          {sensors_list}\n
          ''')

        # today = date.today()
        # date_to = today.replace(day=1)
        # date_from = (date_to - timedelta(days=1)).replace(day=1)

        date_from="2023-01-01"
        date_to="2023-03-31"
        date_format = "%Y-%m-%d"
        date_to = datetime.strptime(date_to, date_format).date()
        date_from = datetime.strptime(date_from, date_format).date()

        print(f"From: {date_from} To: {date_to}")

        with OpenAQ(api_key=OPENAQ_API_KEY, auto_wait=True) as client:
            measurements_list = []

            for sensor_id in sensors_list:
                measurements = extract_measurements_from_sensor_paginated(
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