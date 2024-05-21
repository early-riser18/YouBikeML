import pandas as pd
from predict.forecast_model import RegressionYouBikeModel
from utils.s3_helper import ConnectionToS3
from utils import utils
from utils.sql_utils import DB_Connection, SQL_INSERT_STATEMENT_FROM_DATAFRAME
from sqlalchemy import text
import json
from datetime import datetime, timedelta
from typing import Union
from etl.transform.features_creator import FeaturesCreator_v1

class YoubikeForecastService:
    """Service responsible for providing and maintaining a time-valid forecast.
    Polls DB for valid forecasts, and request new ones if not.

    freshness_thresh: time elapsed until a forecast becomes stale, in minutes .
    """

    _unique_instance = None

    def __init__(self, forecast_model: RegressionYouBikeModel, s3: ConnectionToS3, freshness_thresh: int = 5):
        if YoubikeForecastService._unique_instance is not None:
            raise Exception("This class is a singleton!")
        else:
            self._model = forecast_model
            self._s3 = s3
            self._freshness_thresh = freshness_thresh
            YoubikeForecastService._unique_instance = self

    @classmethod
    def get_instance(cls, freshness_thresh: int = None):
        if cls._unique_instance is None:
            cls._unique_instance = cls(
                RegressionYouBikeModel("model_2024-03-29", FeaturesCreator_v1), ConnectionToS3.from_env(), freshness_thresh
            )
        return cls._unique_instance

    def get_forecast(self, station_ids: list[int]) -> pd.DataFrame:
        """
        Main entrypoint to the object. Returns valid forecasts from database. If expired or not found, refresh them.

        Input
        ------
            station_ids: list of station_ids for which a fill-rate forecast is requested.

        Output
        ------
            forecasts: forecasts in db.fill_rate_forecast schema
        """
        # Validate data
        if len(station_ids) < 1:
            raise ValueError("station_ids parameter must be non-null")

        with DB_Connection.from_env().connection as conn:
            res = conn.execute(text('SELECT "id" from bike_station;')).all()
            all_stations_ids = [x[0] for x in res]
        if set(station_ids) - set(all_stations_ids) != set():
            raise ValueError(
                f"Invalid station ids provided {set(station_ids) - set(all_stations_ids)}"
            )

        existing_valid_forecast = self.__retrieve_valid_forecast_from_db(station_ids)
        stations_to_refresh = list(
            set(station_ids) - set(existing_valid_forecast["station_id"])
        )
        print("No valid forecast found for: ", stations_to_refresh)
        
        fresh_forecast = self.__get_new_forecast(stations_to_refresh)
        if fresh_forecast is not None:
            self.__update_db_fill_rate_forecast(fresh_forecast)

        all_forecasts = self.__retrieve_valid_forecast_from_db(station_ids)
        return all_forecasts

    def __retrieve_valid_forecast_from_db(self, station_ids: list[int]) -> pd.DataFrame:
        """Retrieves forecast from DB and return only valid forecast

        Returns:
            Dataframe with db.fill_rate_forecast schema
        """
        VALIDITY_THRESH = datetime.now() - timedelta(minutes=self._freshness_thresh)
        with DB_Connection.from_env().connection as conn:
            fields = conn.execute(text("SELECT * FROM fill_rate_forecast;")).keys()
            fill_forecasts = pd.DataFrame(columns=fields)
            request_sql = f"SELECT * FROM fill_rate_forecast WHERE station_id in ({', '.join([str(x) for x in station_ids])}) AND run_ts > '{VALIDITY_THRESH}'" + 'ORDER BY "station_id", "relative_ts" ASC;'
            res = conn.execute(text(request_sql))
            fill_forecasts = pd.concat([fill_forecasts, pd.DataFrame(res)])

        return fill_forecasts

    def __update_db_fill_rate_forecast(self, forecasts: pd.DataFrame):
        """
        Input
        -------
            forecasts: Dataframe with db.fill_rate_forecast schema

        Output
        -------
            None
        """
        forecasts["base_ts"] = forecasts["base_ts"].dt.strftime(utils.DB_TS_FORMAT)
        forecasts["run_ts"] = forecasts["run_ts"].dt.strftime(utils.DB_TS_FORMAT)

        statement = SQL_INSERT_STATEMENT_FROM_DATAFRAME(
            forecasts[["station_id", "fill_rate", "relative_ts", "base_ts", "run_ts"]],
            "fill_rate_forecast",
        )
        sql_confict = ' ON CONFLICT ("station_id", "relative_ts", "run_ts") DO NOTHING'
        statement = statement.split(";")[0] + sql_confict + ";"
        with DB_Connection.from_env().connection as conn:
            conn.execute(text(statement))
            conn.commit()

    def __get_new_forecast(self, station_ids: list[int]) -> Union[pd.DataFrame, None]:
        """
        
        Output
        -------
            Returns new forecast with db.fill_rate_forecast schema
        """
        if len(station_ids):
            forecasts_raw = self._model.forecast(station_ids)
            return forecasts_raw
        else:
            return None


def get_fill_rate_forecast(station_id: list[int] = []):
    """ "returns valid fill_rate_forecast
    Output
    ------
        return fill_rate_forecast with db schema
    """
    forecaster = YoubikeForecastService.get_instance(freshness_thresh=5)
    return forecaster.get_forecast(station_id)


if __name__ == "__main__":
    test_station_ids = [
        508201032,
        501208101,
        501216049,
        501210126,
        501209089,
        508201041,
        508201038,
        508201036

    ]
    # service = YoubikeForecastService.get_instance()
    # print(service.get_forecast(test_station_ids))
    print(get_fill_rate_forecast(test_station_ids))