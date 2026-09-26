from openaq import OpenAQ

import os
import pandas as pd
import logging


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


def extract(page=1):
    with OpenAQ(api_key=OPENAQ_API_KEY) as client:
        location = client.locations.list(countries_id=COUNTRY_ID, page=page, limit=LIMIT)

        results = location.results

        print(f"Retrieved {len(results)} entries")
        print(f"Current page: {page}")

        return location

def extract_with_coordinates(lat, lon, radius, page):
    try:
        with OpenAQ(api_key=OPENAQ_API_KEY) as client:
            coordinates = (lat, lon)
            location = client.locations.list(coordinates=coordinates, radius=radius, page=page, limit=LIMIT)
            
            results = location.results

            print(f"Retrieved {len(results)} entries")
            print(f"Current page: {page}")

            return location
    except Exception as e:
        print(e)


def transform(data):
    return data.dict()

def load_to_csv(data, page):


    results = data['results']

    if not results:
        return
    df = pd.json_normalize(results)

    print(f"Loading {len(df)} records from page {page}")
    print('\n')

    # First page: create/overwrite file
    # Subsequent pages: append
    mode = "w" if page == PAGE else "a"
    header = page == PAGE

    df.to_csv(
        CSV_FILE,
        mode=mode,
        header=header,
        index=False,
    )



def main():
    logger = logging.getLogger("openaq")
    logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    logger.addHandler(handler)
    
    try:
        # is_next_page = True
        page = PAGE

        # Optional: start with a clean file
        if os.path.exists(CSV_FILE):
            os.remove(CSV_FILE)

        while True:
            # raw_resp = extract(page) # This option includeds Nakuru's ensors while the requirements want Nairobi's data only.
            raw_resp = extract_with_coordinates(lat=NAIROBI_COORDINATES["lat"], lon=NAIROBI_COORDINATES["lon"], radius=NAIROBI_COORDINATES["radius"], page=page)

            transformed_resp = transform(raw_resp)

            result = transformed_resp["results"]

            if not result:
                print(f"No results on page {page}. Stoping...")
                break

            load_to_csv(transformed_resp, page)
            page += 1

        # while is_next_page:
        #     raw_resp = extract(page)
        #     transformed_resp = transform(raw_resp)
        #     load_to_csv(transformed_resp)

        #     page = transformed_resp['meta'].get("page", 1) + 1
        #     is_next_page = type( transformed_resp['meta'].get("found")) != int #if not int means that we have more resources that we have to pull
        #     # is_next_page = transformed_resp['meta'].get("found").startswith(">")

    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()


'''
header
--------------
"headers": {
    "xRatelimitLimit": 60,
    "xRatelimitRemaining": 59,
    "xRatelimitUsed": 1,
    "xRatelimitReset": 60
  }


meta
-----------------------------
  "meta": {
    "name": "openaq-api",
    "website": "/",
    "page": 1,
    "limit": 100,
    "found": 17
  }



results
---------------
    [
        {
            "id": 6326576,
            "name": "Nairobi, University of Nairobi, Parklands Campus, GEOHealth",
            "locality": null,
            "timezone": "Africa/Nairobi",
            "country": {
                "id": 17,
                "code": "KE",
                "name": "Kenya"
            },
            "owner": {
                "id": 23894,
                "name": "GEOHealth Hub for Eastern Africa"
            },
            "provider": {
                "id": 441,
                "name": "GeoHealth"
            },
            "isMobile": false,
            "isMonitor": true,
            "instruments": [
                {
                "id": 18,
                "name": "BAM 1022"
                }
            ],
            "sensors": [
                {
                "id": 16091678,
                "name": "pm25 µg/m³",
                "parameter": {
                    "id": 2,
                    "name": "pm25",
                    "units": "µg/m³",
                    "displayName": "PM2.5"
                }
                }
            ],
            "coordinates": {
                "latitude": -1.26,
                "longitude": 36.818
            },
            "bounds": [
                36.818,
                -1.26,
                36.818,
                -1.26
            ],
            "distance": null,
            "datetimeFirst": {
                "utc": "2019-08-20T21:00:00Z",
                "local": "2019-08-21T00:00:00+03:00"
            },
            "datetimeLast": {
                "utc": "2023-02-28T20:00:00Z",
                "local": "2023-02-28T23:00:00+03:00"
            }
        }
    ]
'''