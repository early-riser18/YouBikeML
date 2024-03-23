from extraction.weather import WeatherAPI, WeatherConfig, WeatherSnapshot
from prefect import flow, task
from utils.utils import get_formatted_timestamp_as_str
from utils.s3_helper import ConnectionToS3, export_file_to_s3
from prefect.deployments import Deployment
from datetime import date, timedelta


@task(log_prints=True)
def extract_weather_data(
    weather_config: WeatherConfig,
    past_start_date_rel: int = 0,
    past_end_date_rel: int = 0,
    forecast_days: int = None,
) -> WeatherSnapshot:
    """
    Wrapper function to support instanciating a WeatherAPI with a dynamic time range. Required for scheduled flows.
    """
    if forecast_days is None:
        api = WeatherAPI.historic_report(
            weather_config,
            (date.today() - timedelta(days=past_start_date_rel)).strftime("%Y-%m-%d"),
            (date.today() - timedelta(days=past_end_date_rel)).strftime("%Y-%m-%d"),
        )
    else:
        api = WeatherAPI.forecast_report(weather_config, forecast_days)

    res = api.request_data()
    return res


@task(log_prints=True)
def persist_data(
    data: WeatherSnapshot, folder_path: str = "/", file_stub: str = "weather_data_raw"
) -> str:
    """
    Store data to your object storage according to env config.
    """
    s3_co = ConnectionToS3.from_env()
    formatted_extraction_ts = get_formatted_timestamp_as_str(data.extraction_ts)
    file_path = f"{folder_path}/{file_stub}_{formatted_extraction_ts}.csv"
    data_as_csv = data.body.to_csv(index=False)

    upload_path = export_file_to_s3(
        connection=s3_co, file_name=file_path, body=data_as_csv
    )
    return upload_path


@flow(log_prints=True)
def get_weather_report(
    past_start_date_rel: int = None,
    past_end_date_rel: int = None,
    forecast_days: int = None,
) -> str:
    """
    Flow to query weather data. Currently supports either historical report or forecast report. 
    If no forecast_days parameter is passed, then it defaults to getting a historical report.

    Returns
        str : The path where the retrieved weather report has been uploaded.
    """
    w_snapshot = extract_weather_data(
        WeatherConfig(),
        past_start_date_rel=past_start_date_rel,
        past_end_date_rel=past_end_date_rel,
        forecast_days=forecast_days,
    )

    if forecast_days is not None:
        file_stub = "weather_forecast_report"
        folder_path = "raw_data/weather/forecast_report"
    else:
        file_stub = "weather_historical_report"
        folder_path = "raw_data/weather/historical_report"

    file_stub = (
        "weather_forecast_report" if forecast_days is not None else "weather_historical_report"
    )
    upload_path = persist_data(w_snapshot, folder_path, file_stub)
    return upload_path


if __name__ == "__main__":

    if input("Run flow locally [type yes]") == "yes":
        get_weather_report(past_start_date_rel=20, past_end_date_rel=14)
        get_weather_report(forecast_days=3) 

    elif input("Deploy this flow [type yes]") == "yes":
        print("Deploying...")
        a = Deployment.build_from_flow(
            flow=get_weather_report,
            output=f"flows/get_weather_historical_report.yaml",
            name="get_weather_historical_report",
            work_pool_name="ecs-stage",
            work_queue_name="default",
            schedules=[
                {
                    "schedule": {
                        "interval": 604800,
                        "anchor_date": "2024-03-28T00:00:00+08:00",
                        "timezone": "Asia/Taipei",
                    },
                    "active": True,
                }
            ],
            parameters={"past_start_date_rel": 13, "past_end_date_rel": 7},
            path="/opt/prefect/flows",
            apply=True,
            load_existing=False,
        )
        b = Deployment.build_from_flow(
            flow=get_weather_report,
            output=f"flows/get_weather_forecast_report.yaml",
            name="get_weather_forecast_report",
            work_pool_name="ecs-stage",
            work_queue_name="default",
            schedules=[
                {
                    "schedule": {
                        "interval": 86400,
                        "anchor_date": "2024-03-08T00:00:00+08:00",
                        "timezone": "Asia/Taipei",
                    },
                    "active": True,
                }
            ],
            parameters={"forecast_days": 1},
            path="/opt/prefect/flows",
            apply=True,
            load_existing=False,
        )