from retry_requests import retry
import openmeteo_requests
from openmeteo_sdk.Variable import Variable
from openmeteo_sdk.WeatherApiResponse import WeatherApiResponse
import pandas as pd
import numpy as np
import pytz
from datetime import datetime


class WeatherSnapshot:
    def __init__(
        self,
        extraction_ts: datetime,
        body: pd.DataFrame,
    ):
        self.extraction_ts = extraction_ts
        self.body = body


class WeatherConfig:
    LOCATIONS = {
        "TaiBei": [25.05, 121.54],
        "XinBei": [25.01, 121.46],
        "Taoyuan": [24.98, 121.28],
        "XinZhu": [24.81, 120.99],
        "MiaoLi": [24.57, 120.70],
        "TaiZhong": [24.14, 120.67],
        "JiaYi": [23.47, 120.44],
        "TaiNan": [22.99, 120.20],
        "GaoXiong": [22.63, 120.32],
        "PingDong": [22.67, 120.48],
    }

    REQUESTED_FIELDS = [
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


class WeatherAPI:

    @classmethod
    def historic_report(cls, config, start_date, end_date):
        return cls(
            config=config,
            url_endpoint="https://archive-api.open-meteo.com/v1/archive",
            start_date=start_date,
            end_date=end_date,
        )

    @classmethod
    def forecast_report(cls, config, forecast_days):
        return cls(
            config=config,
            url_endpoint="https://api.open-meteo.com/v1/forecast",
            forecast_days=forecast_days,
        )

    def __init__(
        self,
        config: WeatherConfig,
        url_endpoint: str,
        start_date: str | None = None,
        end_date: str | None = None,
        forecast_days: int | None = None,
    ):
        self._locations = config.LOCATIONS
        self._fields = config.REQUESTED_FIELDS
        self._endpoint_url = url_endpoint

        stations_lat = [s[0] for s in self._locations.values()]
        stations_lng = [s[1] for s in self._locations.values()]

        self._request_params = {
            "latitude": stations_lat,
            "longitude": stations_lng,
            "timeformat": "unixtime",
            "timezone": "Asia/Singapore",
            "hourly": config.REQUESTED_FIELDS,
        }
        if forecast_days:
            self._request_params["forecast_days"] = forecast_days
        else:
            self._request_params["start_date"] = start_date
            self._request_params["end_date"] = end_date

    @property
    def raw_response(self) -> list[WeatherApiResponse]:
        return self._raw_response

    def _request_data(self) -> list[WeatherApiResponse]:
        """
        Private method to call api endpoint. Response format is dependent on API.
        """
        retry_session = retry(retries=5, backoff_factor=0.2)
        openmeteo = openmeteo_requests.Client(session=retry_session)
        responses = openmeteo.weather_api(
            self._endpoint_url, params=self._request_params
        )
        self._raw_response = responses
        return responses

    def _consolidate_weather_snapshots(
        self, snapshots: list[WeatherSnapshot]
    ) -> WeatherSnapshot:
        weather_dfs = [s.body for s in snapshots]
        consolidated_body = pd.concat(weather_dfs, ignore_index=True)
        consolidated_w_snapshot = WeatherSnapshot(
            extraction_ts=snapshots[0].extraction_ts, body=consolidated_body
        )
        return consolidated_w_snapshot

    def _process_raw_snapshot(self, snapshot) -> WeatherSnapshot:
        """
        Constructs a WeatherSnapshot object from the API native response object WeatherApiResponse.
        Appends with location and time range.
        """
        # Extract Data from each snapshot
        snapshot_dic = {}
        hourly = snapshot.Hourly()

        for i in list(
            map(lambda i: hourly.Variables(i), range(0, hourly.VariablesLength()))
        ):
            key = next(
                name for name, value in vars(Variable).items() if value == i.Variable()
            )
            snapshot_dic[key] = i.ValuesAsNumpy()

        # Add coordinates of the data collected
        array_shape = hourly.Variables(0).ValuesAsNumpy().shape
        snapshot_dic["lat"] = np.full(array_shape, snapshot.Latitude())
        snapshot_dic["lng"] = np.full(array_shape, snapshot.Longitude())

        # Add time of extraction
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

        # Build WeatherSnapshot object
        tz_tst = pytz.timezone("Asia/Taipei")
        time_now = datetime.today().now(tz=tz_tst)
        weather_object = WeatherSnapshot(
            extraction_ts=time_now, body=pd.DataFrame(snapshot_dic)
        )
        return weather_object

    def request_data(self) -> WeatherSnapshot:
        """
        Calls the Open Meteo API, transforms the data according to interface and returns it.

            Parameters:
                None

            Returns:
                WeatherSnapshot for all locations requested.
        """
        raw_response = self._request_data()
        weather_snapshots = [self._process_raw_snapshot(r) for r in raw_response]
        consolidated_snapshot = self._consolidate_weather_snapshots(weather_snapshots)
        return consolidated_snapshot


if __name__ == "__main__":

    print("Running...\n")

    api = WeatherAPI.historic_report(WeatherConfig(), "2024-03-01", "2024-03-07")
    res = api.request_data()
    res.body.to_csv("dfd.csv")
