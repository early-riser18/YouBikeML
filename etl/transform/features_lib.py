import pandas as pd
import pyspark.sql
from typing import Union
from abc import ABC, abstractmethod
from pyspark.sql.functions import month, dayofweek, hour
from etl.extraction.youbike import extract_youbike_raw_data
from etl.transform.clean_youbike_data import clean_youbike_data
from utils.utils import (
    get_youbike_snapshot_data_for_time_range,
    get_weather_zone,
    get_latest_weather_data,
)
import numpy as np


class DataTransformer(ABC):
    """Data transformation base class. Enables to run code with pandas or spark depending on the job size.
    Implementations via both pandas and spark within one object allow to prevent error when implementing the
    transformation in the other library.
    """

    def __init__(self, exec_library):
        self._exec_library = exec_library

    @abstractmethod
    def in_pandas(self):
        pass

    @abstractmethod
    def in_pyspark(self):
        pass

    def run(self, *args, **kwargs) -> Union[pd.DataFrame, pyspark.sql.DataFrame]:
        match self._exec_library:
            case "pandas":
                return self.in_pandas(*args, **kwargs)
            case "pyspark":
                return self.in_pyspark(*args, **kwargs)
            case _:
                raise ValueError(f"Unknown exec_library value: {self._exec_library}")


class MakeWeatherFeatures(DataTransformer):

    def in_pandas(self, df):
        df["1h_fwd_apparent_temperature"] = (
            df.sort_values(by=["zone", "datetime"])
            .groupby("zone")["apparent_temperature"]
            .transform(lambda x: x.shift(-1))
        )

        df["1h_fwd_precipitation"] = (
            df.sort_values(by=["zone", "datetime"])
            .groupby("zone")["precipitation"]
            .transform(lambda x: x.shift(-1))
        )

        return df

    def in_pyspark(self):
        pass


class LagFeatures(DataTransformer):
    def in_pandas(self, df):

        df["30m_blag_pct_full"] = (
            df.sort_values(by="extraction_ts")
            .groupby(by=["id"])["pct_full"]
            .transform(lambda x: x.shift(3))  # Assumes each record is 10 mins
        )

        df["120m_avg_pct_full"] = (
            df.sort_values(by="extraction_ts")
            .groupby(by=["id"])["pct_full"]
            .transform(
                lambda x: x.rolling(window=12).mean()
            )  # Assumes each record is 10 mins
        )

        return df

    def in_pyspark(self):
        pass


class TimeFeatures(DataTransformer):
    def in_pandas(self, df):
        df["month"] = df["extraction_ts"].dt.month
        df["day_of_week"] = df["extraction_ts"].dt.weekday
        df["hour"] = df["extraction_ts"].dt.hour
        return df

    def in_pyspark(self, df):
        df = (
            df.withColumn("month", month("extraction_ts"))
            .withColumn("day_of_week", dayofweek("extraction_ts"))
            .withColumn("hour", hour("extraction_ts"))
        )
        return df


class StationOccupancyFeatures(DataTransformer):
    def in_pandas(self, df):
        df["pct_full"] = df["full"] / (df["full"] + df["empty"])
        return df

    def in_pyspark(self):
        pass


class FormatToFeaturesSchema(DataTransformer):
    def __init__(self, exec_library):
        super().__init__(exec_library)

    def in_pandas(self, df: pd.DataFrame):
        df = df.set_index(keys=["id", "extraction_ts"]).sort_index()
        df = df[
            [
                "pct_full",
                "month",
                "day_of_week",
                "hour",
                "30m_blag_pct_full",
                "120m_avg_pct_full",
                "apparent_temperature",
                "precipitation",
                "wind_speed",
                "1h_fwd_apparent_temperature",
                "1h_fwd_precipitation",
            ]
        ].copy(deep=True)
        return df

    def in_pyspark(self):
        pass


# IDEALLY THIS IS A VALIDATION CLASS
class ValidateFeaturesSchema(DataTransformer):

    def __init__(self, exec_library):
        super().__init__(exec_library)
        ### PROBLEM WITH IT. DESPITE NOT HAVING first 2 COLUMNS, IT PASSES
        self.PREDICTION_FEATURES_SCHEMA = {
            "id": np.int64,
            "extraction_ts": pd.DatetimeTZDtype(unit="ms", tz="Asia/Taipei"),
            "pct_full": np.float64,
            "month": np.int32,
            "day_of_week": np.int32,
            "hour": np.int32,
            "30m_blag_pct_full": np.float64,
            "120m_avg_pct_full": np.float64,
            "apparent_temperature": np.float32,
            "precipitation": np.float32,
            "wind_speed": np.float32,
            "1h_fwd_apparent_temperature": np.float32,
            "1h_fwd_precipitation": np.float32,
        }

    def in_pandas(self, df):
        print("validation: ", df.shape)
        try:
            for c in df.columns:
                assert df[c].dtype == self.PREDICTION_FEATURES_SCHEMA[c]
                print(f"{c} as {df[c].dtype}: OK")
        except:
            return False
        return True

    def in_pyspark(self):
        pass


class CreateInputPredictionFeatures(DataTransformer):
    """ "
    Fetches and prepare the data required for the Prediction Features
    """

    def in_pandas(self, station_ids: list[int]):
        # Extract latest youbike snapshot
        #TODO: GET Freshest current data from endpoint
        fresh_youbike_data_clean = clean_youbike_data(extract_youbike_raw_data().body)

        # pull last 120 mins snapshot for lag features
        fresh_extraction_ts = fresh_youbike_data_clean["extraction_ts"][0]
        historic_youbike_data = get_youbike_snapshot_data_for_time_range(
            oldest_ts=fresh_extraction_ts - pd.Timedelta(minutes=120),
            newest_ts=fresh_extraction_ts,
        )

        concat_youbike = pd.concat(
            [fresh_youbike_data_clean, historic_youbike_data], ignore_index=True
        )
        # Keep requested stations only
        concat_youbike = concat_youbike[concat_youbike["id"].isin(station_ids)].copy(
            deep=True
        )

        # merge weather zone and youbike
        weather_zones = get_weather_zone()
        weather_zones.rename(
            columns={"name": "weather_zone", "id": "weather_zone_id"}, inplace=True
        )
        main_df = pd.merge(
            left=concat_youbike,
            right=weather_zones[["weather_zone_id", "weather_zone"]],
            how="left",
            on="weather_zone_id",
        )

        # Get weather data
        weather_data = get_latest_weather_data()

        weather_data = MakeWeatherFeatures("pandas").run(weather_data)
        weather_data = weather_data.drop(["lat", "lng"], axis=1).rename(
            columns={"zone": "weather_zone"}
        )
        main_df["extraction_ts"] = pd.to_datetime(main_df["extraction_ts"], utc=True)
        print(main_df["extraction_ts"].dtype, main_df.info())
        main_df["y_m_d_h"] = main_df["extraction_ts"].dt.strftime("%Y-%m-%d_%H")
        weather_data["y_m_d_h"] = weather_data["datetime"].dt.strftime("%Y-%m-%d_%H")

        main_df = pd.merge(
            left=main_df,
            right=weather_data[
                [
                    "temperature",
                    "relative_humidity",
                    "apparent_temperature",
                    "precipitation",
                    "wind_speed",
                    "y_m_d_h",
                    "zone",
                    "1h_fwd_precipitation",
                    "1h_fwd_apparent_temperature",
                ]
            ],
            on=["zone", "y_m_d_h"],
            how="left",
        )
        return main_df

    def in_pyspark(self):
        pass
