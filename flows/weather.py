from retry_requests import retry
import openmeteo_requests
from openmeteo_sdk.Variable import Variable
import pandas as pd
import numpy as np
import pytz
from datetime import datetime
from functools import reduce
from utils.utils import get_formatted_timestamp_as_str
from utils.s3_helper import ConnectionToS3, export_csv_to_s3
from prefect import flow, task
from prefect.deployments import Deployment


class WeatherSnapshot:
    def __init__(
        self,
        extraction_ts: datetime,
        body: pd.DataFrame,
    ):
        self.extraction_ts = extraction_ts
        self.body = body

@task(log_prints=True)
def get_weather_data() -> WeatherSnapshot:
    """Returns the requested data from the weather API"""
    retry_session = retry(retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    url = "https://api.open-meteo.com/v1/forecast"
    requested_fields = [
        "temperature_2m",
        "relative_humidity_2m",
        "apparent_temperature",
        "precipitation_probability",
        "precipitation",
        "rain",
        "showers",
        "wind_speed_10m",
        "wind_gusts_10m",
    ]
    # Nanjing Fuxing station (25.05, 121.54)
    # Banqiao station (25.01, 121.46)
    params = {
        "latitude": [25.05, 25.01],
        "longitude": [121.54, 121.46],
        "hourly": requested_fields,
        "timeformat": "unixtime",
        "timezone": "Asia/Singapore",
        "past_days": 1,
        "forecast_days": 2,
    }
    responses = openmeteo.weather_api(url, params=params)
    return responses


def create_object_from_response(data: any) -> WeatherSnapshot:
    """
    Constructs a WeatherSnapshot object from the API native response object WeatherApiResponse.
    Appends with location and time range.
    """
    # Extract Data from each snapshot
    extracted_data = []
    for snapshot in data:
        snapshot_dic = {}
        hourly = snapshot.Hourly()

        for i in list(
            map(lambda i: hourly.Variables(i), range(0, hourly.VariablesLength()))
        ):
            key = next(
                name for name, value in vars(Variable).items() if value == i.Variable()
            )
            snapshot_dic[key] = i.ValuesAsNumpy()

        array_shape = hourly.Variables(0).ValuesAsNumpy().shape
        snapshot_dic["lat"] = np.full(array_shape, snapshot.Latitude())
        snapshot_dic["lng"] = np.full(array_shape, snapshot.Longitude())

        snapshot_dic["datetime"] = (
            pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s"),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s"),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left",
            )
            .tz_localize("UTC")
            .tz_convert("Asia/Taipei")
        )
        extracted_data.append(snapshot_dic)

    # Append snapshots into one object
    consolidated_data = {}
    for key in extracted_data[0]:
        consolidated_data[key] = reduce(np.append, [j[key] for j in extracted_data])

    tz_tst = pytz.timezone("Asia/Taipei")
    time_now = datetime.today().now(tz=tz_tst)
    weather_object = WeatherSnapshot(
        extraction_ts=time_now, body=pd.DataFrame(consolidated_data)
    )
    return weather_object

@task(log_prints=True)
def persist_data(data: WeatherSnapshot) -> str:
    s3_co = ConnectionToS3.from_env()
    formated_ts = get_formatted_timestamp_as_str(data.extraction_ts)
    file_path = f"raw_data/weather_data_raw_{formated_ts}.csv"
    data_as_csv = data.body.to_csv(index=False)
    r = export_csv_to_s3(connection=s3_co, file_name=file_path, body=data_as_csv)
    return r

@flow(log_prints=True)
def extract_weather_data():
    weather_response = get_weather_data()
    weather_snapshot_obj = create_object_from_response(weather_response)
    r = persist_data(weather_snapshot_obj)
    print("Extracted data uploaded at ", r)

if __name__ == "__main__":

    if input("Run this flow locally? [type yes]") == "yes":
        print("Running...\n", extract_weather_data())

    if input("Deploy this flow [type yes]") == "yes":
        print("Deploying...")
        a = Deployment.build_from_flow(
            flow=extract_weather_data,
            output=f"flows/extract-weather-data.yaml",
            name="extract_weather-stage",
            work_pool_name="ecs-stage",
            work_queue_name="default",
            schedules=[
                {
                    "schedule": {
                        "interval": 86400.0,
                        "anchor_date": "2024-03-08T00:00:00+00:00",
                        "timezone": "Asia/Taipei",
                    },
                    "active": True,
                }
            ],
            path="/opt/prefect/flows",
            apply=True,
            load_existing=False,
        )
