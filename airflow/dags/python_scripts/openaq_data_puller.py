# extract_locations → extract_sensors → extract_measurements → validate_measurements → load_to_postgres.
from openaq import OpenAQ
from openaq.core.exceptions import RateLimitError, HTTPRateLimitError, ApiKeyMissingError
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



def extract_locations_with_coordinates(lat, lon, radius, page=1):
    try:
        with OpenAQ(api_key=OPENAQ_API_KEY) as client:
            coordinates = (lat, lon)
            print(coordinates)
            location = client.locations.list(coordinates=coordinates, radius=radius, page=page, limit=LIMIT)
            
            results = location.results

            print(f"Retrieved {len(results)} entries")
            print(f"Current page: {page}")

            return location.dict()

    except (RateLimitError, HTTPRateLimitError):
        print(
            "Rate limit reached;"
            "waiting 60 seconds before retrying"
        )
        time.sleep(60)

    except ApiKeyMissingError as e:
        print(f"Fatal: OpenAQ API key is missing. Error: {e}")
        raise SystemExit(1)

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
        
    except (RateLimitError, HTTPRateLimitError):
        print(
            "Rate limit reached; "
            "waiting 60 seconds before retrying"
        )
        time.sleep(60)

    except ApiKeyMissingError as e:
        print(f"Fatal: OpenAQ API key is missing. Error: {e}")
        raise SystemExit(1)


def extract_measurements_from_sensor_paginated(
    sensor_id,
    location_id,
    location_name,
    date_from,
    date_to,
):
    resp = []
    page = PAGE

    with OpenAQ(api_key=OPENAQ_API_KEY, auto_wait=True) as client:

        while True:

            try:
                measurements = client.measurements.list(
                    sensors_id=sensor_id,
                    data="measurements",
                    datetime_from=date_from,
                    datetime_to=date_to,
                    page=page,
                    limit=LIMIT,
                )

            except (RateLimitError, HTTPRateLimitError):
                print(
                    "Rate limit reached; "
                    "waiting 60 seconds before retrying"
                )
                time.sleep(60)
                continue

            except ApiKeyMissingError as e:
                print(f"Fatal: OpenAQ API key is missing. Error: {e}")
                raise SystemExit(1)

            measurements_results = measurements.dict()["results"]

            if not measurements_results:
                print(
                    f"No results on page {page}. "
                    "Exiting..."
                )
                break

            rows = []

            for measurement in measurements_results:
                rows.append({
                    "location_id": location_id,
                    "location_name": location_name,
                    "sensor_id": sensor_id,
                    "parameter": measurement["parameter"]["name"],
                    "unit": measurement["parameter"]["units"],
                    "value": measurement["value"],
                    "measurement_timestamp": (
                        measurement["period"]["datetime_from"]["utc"]
                    ),
                })

            resp.extend(rows)

            page += 1

    return resp