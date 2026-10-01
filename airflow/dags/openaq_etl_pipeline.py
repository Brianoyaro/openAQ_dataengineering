from airflow.sdk import dag, task, get_current_context
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

from pendulum import datetime
import os
import json
import tempfile
from pathlib import Path
# from datetime import datetime

from dotenv import load_dotenv

from dags.python_scripts.openaq_data_puller import (
    extract_locations_with_coordinates,
    extract_result,
    extract_sensors_from_location,
    extract_measurements_from_sensor_paginated,
    get_all_parameters,
)


# Configuration

env_path = os.path.join(
    os.path.dirname(__file__),
    "..",
    ".env",
)

load_dotenv(env_path)


OPENAQ_API_KEY = os.getenv("OPEN_API_KEY")

LATITUDE = os.getenv("NAIROBI_LATITUDE")
LONGITUDE = os.getenv("NAIROBI_LONGITUDE")
RADIUS = os.getenv("NAIROBI_RADIUS")
PAGE = int(os.getenv("PAGE", 1))


# DATE_FROM = "2026-08-01" #TODO
# DATE_TO = "2026-09-25" #TODO


RAW_DIR = Path("/usr/local/airflow/data/raw/openaq")
VALIDATED_DIR = Path("/usr/local/airflow/data/validated/openaq")



@dag(
    dag_id="open_aq_etl_pipeline",
    start_date=datetime(2025, 4, 22),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["openaq", "etl"],
    template_searchpath=[
        "/usr/local/airflow/sql_scripts"
    ],
)
def open_aq_etl_pipeline():

    # 1. CREATE DATABASE TABLES

    create_tables = SQLExecuteQueryOperator(
        task_id="create_raw_etl_table_if_absent",
        conn_id="postgres_conn",
        sql="create.sql",
    )

    create_parameter_table = SQLExecuteQueryOperator(
            task_id="create_parameters_table_if_absent",
            conn_id="postgres_conn",
            sql="create_parameters.sql",
        )


    # 2. EXTRACT LOCATIONS

    @task
    def extract_locations():

        response = extract_locations_with_coordinates(
            lat=float(LATITUDE),
            lon=float(LONGITUDE),
            radius=int(RADIUS),
            page=PAGE,
        )

        locations = extract_result(response)

        print(f"Extracted {len(locations)} locations")
        print(f"Locations response: {locations}")

        return locations


    # 3. EXTRACT SENSORS

    @task
    def extract_sensors(locations):

        sensors_list = []

        for location in locations:

            location_id = location["id"]
            location_name = location["name"]

            print(
                f"Extracting sensors for "
                f"location={location_id}, "
                f"name={location_name}"
            )

            sensors = extract_sensors_from_location(
                (location_id, location_name)
            )

            if not sensors:
                print(
                    f"No sensors found for "
                    f"location {location_id}"
                )
                continue

            for sensor in sensors:

                sensors_list.append(
                    {
                        "sensor_id": sensor["id"],
                        "location_id": location_id,
                        "location_name": location_name,
                    }
                )

        print(
            f"Discovered {len(sensors_list)} sensors"
        )

        return sensors_list


    # 4. EXTRACT MEASUREMENTS
    # One mapped task instance = one sensor

    @task
    def extract_measurements(sensor):

        sensor_id = sensor["sensor_id"]

        print(
            f"Extracting measurements "
            f"for sensor {sensor_id}"
        )

        context = get_current_context()
        
        data_interval_start = context["data_interval_start"]
        data_interval_end = context["data_interval_end"]

        #############################################333
        '''
        I can switch this with:
         replacing 
           schedule=@daily 
         with schedule=CronDataIntervalTimetable(
                "0 0 * * *",
                timezone="UTC",
            )
        '''
        if data_interval_end == data_interval_start:
            data_interval_end = data_interval_start.add(days=1)
        #################################################

        DATE_FROM = data_interval_start.to_iso8601_string() 
        DATE_TO = data_interval_end.to_iso8601_string()
        print(f"\n\ncontext: {context}\nDATE_FROM: {DATE_FROM}\nDATE_TO: {DATE_TO}")
        measurements = (
            extract_measurements_from_sensor_paginated(
                sensor_id=sensor_id,
                location_id=sensor["location_id"],
                location_name=sensor["location_name"],
                date_from=DATE_FROM,
                date_to=DATE_TO,
            )
        )

        print(
            f"Sensor {sensor_id}: "
            f"extracted {len(measurements)} measurements"
        )

        # This approach ensures that the same file name is used for retries -in a given duratio- to preventing creating several duplicate files

        raw_dir = RAW_DIR / (
            f"date_from={DATE_FROM}/"
            f"date_to={DATE_TO}"
        )

        raw_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        raw_file = (
            raw_dir /
            f"sensor_{sensor_id}.jsonl"
        )

        # Write atomically.
        #
        # First write to temporary file.
        # Then replace the final file.

        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            delete=False,
            dir=raw_dir,
            prefix=f"sensor_{sensor_id}_",
            suffix=".tmp",
        ) as tmp:

            tmp_path = Path(tmp.name)

            for measurement in measurements:

                tmp.write(
                    json.dumps(
                        measurement,
                        ensure_ascii=False,
                    )
                    + "\n"
                )

        os.replace(
            tmp_path,
            raw_file,
        )

        print(
            f"Raw file written: {raw_file}"
        )

        # Only the file path goes through XCom. instead of openAQ mesurement data which is quite huge

        return str(raw_file)


    # 5. VALIDATE MEASUREMENTS
    # One mapped task instance = one raw sensor file

    @task
    def validate_measurements(raw_file):

        raw_file = Path(raw_file)

        print(
            f"Validating {raw_file}"
        )

        context = get_current_context()
                
        data_interval_start = context["data_interval_start"]
        data_interval_end = context["data_interval_end"]

        #############################################333
        '''
        I can switch this with:
         replacing 
           schedule=@daily 
         with schedule=CronDataIntervalTimetable(
                "0 0 * * *",
                timezone="UTC",
            )
        '''
        if data_interval_end == data_interval_start:
            data_interval_end = data_interval_start.add(days=1)
        #################################################
        
        DATE_FROM = data_interval_start.to_iso8601_string() 
        DATE_TO = data_interval_end.to_iso8601_string()

        validated_dir = VALIDATED_DIR / (
            f"date_from={DATE_FROM}/"
            f"date_to={DATE_TO}"
        )

        validated_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        validated_file = (
            validated_dir /
            raw_file.name
        )

        valid_count = 0
        invalid_count = 0

        with (
            open(
                raw_file,
                "r",
                encoding="utf-8",
            ) as source,
            tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                delete=False,
                dir=validated_dir,
                prefix="validated_",
                suffix=".tmp",
            ) as tmp
        ):

            tmp_path = Path(tmp.name)

            for line in source:

                measurement = json.loads(line)

                # Validation rules

                if measurement.get("sensor_id") is None:
                    invalid_count += 1
                    continue

                if measurement.get("location_id") is None:
                    invalid_count += 1
                    continue

                if measurement.get("parameter") is None:
                    invalid_count += 1
                    continue

                if measurement.get("unit") is None:
                    invalid_count += 1
                    continue

                if measurement.get("value") is None:
                    invalid_count += 1
                    continue

                if measurement.get(
                    "measurement_timestamp"
                ) is None:
                    invalid_count += 1
                    continue

                # Try to verify the timestamp.

                try:

                    datetime.fromisoformat(
                        measurement[
                            "measurement_timestamp"
                        ].replace(
                            "Z",
                            "+00:00",
                        )
                    )

                except ValueError:

                    invalid_count += 1
                    continue

                # Valid measurement

                tmp.write(
                    json.dumps(
                        measurement,
                        ensure_ascii=False,
                    )
                    + "\n"
                )

                valid_count += 1

        os.replace(
            tmp_path,
            validated_file,
        )

        print(
            f"Validation complete: "
            f"valid={valid_count}, "
            f"invalid={invalid_count}"
        )

        return str(validated_file)


    # 6. LOAD POSTGRESQL
    # One mapped task instance = one validated sensor file

    @task
    def load_to_postgres(validated_file):

        validated_file = Path(validated_file)

        print(
            f"Loading {validated_file} "
            f"into PostgreSQL"
        )

        hook = PostgresHook(
            postgres_conn_id="postgres_conn"
        )

        connection = hook.get_conn()

        try:

            with connection.cursor() as cursor:

                sql = """
                    INSERT INTO raw_air_quality (
                        location_id,
                        location_name,
                        sensor_id,
                        parameter_id,
                        unit,
                        value,
                        measurement_timestamp
                    )
                    VALUES (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
                    )
                    ON CONFLICT (
                        sensor_id,
                        parameter_id,
                        measurement_timestamp
                    )
                    DO NOTHING;
                """

                rows = []

                with open(
                    validated_file,
                    "r",
                    encoding="utf-8",
                ) as file:

                    for line in file:

                        measurement = json.loads(line)

                        rows.append(
                            (
                                measurement["location_id"],
                                measurement["location_name"],
                                measurement["sensor_id"],
                                measurement["parameter_id"],
                                measurement["unit"],
                                measurement["value"],
                                measurement[
                                    "measurement_timestamp"
                                ],
                            )
                        )

                if rows:

                    cursor.executemany(
                        sql,
                        rows,
                    )

                    connection.commit()

                    print(
                        f"Loaded {len(rows)} "
                        f"rows from {validated_file}"
                    )

                else:

                    print(
                        f"No valid rows in "
                        f"{validated_file}"
                    )

        except Exception:

            connection.rollback()

            raise

        finally:

            connection.close()



    @task
    def load_to_postgres_parameters():

        parameters = get_all_parameters()

        print(
            f"Loading {len(parameters)} parameters "
            f"into PostgreSQL"
        )

        hook = PostgresHook(
            postgres_conn_id="postgres_conn"
        )

        connection = hook.get_conn()

        try:

            with connection.cursor() as cursor:

                sql = """
                    INSERT INTO parameters (
                        parameter_id,
                        parameter_name,
                        parameter_units,
                        parameter_display_name,
                        parameter_description
                    )
                    VALUES (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
                    )
                    ON CONFLICT (
                        parameter_id
                    )
                    DO NOTHING;
                """

                rows = []

                for parameter in parameters:
                    rows.append(
                        (
                            parameter.id,
                            parameter.name,
                            parameter.units,
                            parameter.display_name,
                            parameter.description,
                        )
                    )

                if rows:

                    cursor.executemany(
                        sql,
                        rows,
                    )

                    connection.commit()

                    print(f"Loaded {len(rows)} parameters to the database")

                else:

                    print("Failed to load parameters to the database")

        except Exception:

            connection.rollback()

            raise

        finally:

            connection.close()

    dbt_transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=ProjectConfig(
            dbt_project_path="/usr/local/airflow/dbt/open_aq",
        ),
        profile_config=ProfileConfig(
            profile_name="open_aq",
            target_name="dev",
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id="postgres_conn",
                profile_args={
                    "schema": "public",
                },
            ),
        ),
    )


    # PIPELINE
    locations = extract_locations()

    sensors = extract_sensors(
        locations
    )

    raw_files = extract_measurements.expand(
        sensor=sensors
    )

    validated_files = validate_measurements.expand(
        raw_file=raw_files
    )

    loaded_measurements = load_to_postgres.expand(
        validated_file=validated_files
    )

    loaded_parameters = load_to_postgres_parameters()


    # DEPENDENCIES
    create_tables >> create_parameter_table
    create_parameter_table >> loaded_parameters
    loaded_parameters >> locations

    locations >> sensors

    sensors >> raw_files

    raw_files >> validated_files

    validated_files >> loaded_measurements

    loaded_measurements >> dbt_transform


open_aq_etl_pipeline()