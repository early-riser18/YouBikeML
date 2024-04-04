import pandas as pd
from predict.forecast_model import RegressionYouBikeModel
from utils.s3_helper import ConnectionToS3
from abc import abstractclassmethod
import json


class YoubikeForecastService:
    """Service responsible for providing and maintaining a time-valid forecast.
    Polls DB for valid forecasts, and request new ones if not.
    """

    _unique_instance = None

    def __init__(self, forecast_model: RegressionYouBikeModel, s3: ConnectionToS3):
        if YoubikeForecastService._unique_instance is not None:
            raise Exception("This class is a singleton!")
        else:
            self._model = forecast_model
            self._s3 = s3
            YoubikeForecastService._unique_instance = self

    @classmethod
    def get_instance(cls):
        if cls._unique_instance is None:
            cls._unique_instance = cls(
                RegressionYouBikeModel(), ConnectionToS3.from_env()
            )
        return cls._unique_instance

    def get_forecast(self, station_ids: list[int]) -> str:
        """
        Main entrypoint to the object. Checks if DB has forecasts, otherwise creates new ones.
        Returns results serialized as JSON.
        """
        # Check if station is in DB
        # For stations not in DB, make forecast
        fresh_forecast = self.__refresh_forecast(station_ids)
        return fresh_forecast.to_json()

    def __retrieve_forecast_from_db(self, station_ids: list[int]):
        ## SHOULD BE A WRAPPER THAT deserialize DB output into a StationForecast Object
        pass

    def __refresh_forecast(self, station_ids: list[int]):
        forecasts_raw = self._model.forecast(station_ids)
        return forecasts_raw


def lambda_handler(event: dict, context):
    """Wrapper function to handle calls to YoubikeForecastService via AWS Lambda API"""
    event_body = json.loads(event['body'])
    service = YoubikeForecastService.get_instance()
    res = service.get_forecast(event_body["station_id"])
    return res


if __name__ == "__main__":
    test_station_ids = [
        508201032,
        501208101,
        501216049,
        501210126,
        501209089,
        508201041,
    ]

   
    service = YoubikeForecastService.get_instance()
    print(service.get_forecast(test_station_ids))