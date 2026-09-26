from airflow.sdk import dag, task
import pendulum
import os
from dotenv import load_dotenv
from python_scripts.openAQ import extract_with_coordinates, extract_result, extract_sensors_from_location, extract_measurements_from_sensor_paginated

env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(env_path)


OPENAQ_API_KEY = os.getenv("OPEN_API_KEY")


csv_file = 'temp_measurements.csv'
PAGE = 1
LIMIT = 1000
COUNTRY_ID = 17 #Kenya
NAIROBI_COORDINATES = {
    "lat": -1.286389,
    'lon': 36.817223,
    'radius': 25000 #25KM
}



@dag(
    start_date=datetime(2025, 4, 22),
    schedule="@daily",
    tags=["openAQ"],
)
def run_me():
    @task
    def extract():
        print("extracted")

    @task
    def transform():
        print("transformed")

    @task
    def load():
        print("loaded")

    extract() >> transform() >>load()

run_me()