from airflow.sdk import dag, task
import pendulum
import os
from dotenv import load_dotenv
from python_scripts.openAQ import extract_with_coordinates, extract_result, extract_sensors_from_location, extract_measurements_from_sensor_paginated

env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(env_path)


OPENAQ_API_KEY = os.getenv("OPEN_API_KEY")
LONGITUDE = os.getenv("NAIROBI_LATITUDE")
LATITUDE = os.getenv("NAIROBI_LONGITUDE")
RADIUS = os.getenv("NAIROBI_RADIUS")
PAGE = os.getenv("PAGE", 1)




@dag(
    start_date=datetime(2025, 4, 22),
    schedule="@daily",
    tags=["openAQ"],
)
def run_me():
    @task
    def extract():
        print("extracted")
        resp = extract_with_coordinates(lat=LATITUDE,lon=LONGITUDE,radius=RADIUS,page=PAGE)
        response = extract_result(resp)
        print(response)
        return response

    @task
    def transform():
        print("transformed")

    @task
    def load():
        print("loaded")

    response = extract()
    transform() >>load()

run_me()