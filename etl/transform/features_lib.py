import pyspark
import pandas as pd
import numpy as np
from typing import Union
from abc import ABC, abstractmethod
from pyspark.sql.functions import (
    month,
    dayofweek,
    hour,
    from_unixtime,
    lead,
    date_format,
    isnan,
    lag,
    mean,
)
from utils.utils import (
    get_youbike_snapshot_data_for_time_range,
    get_weather_zone,
    get_latest_weather_data,
    DB_Connection,
)
from pyspark.sql.window import Window
from db.main import get_all_valid_stations_id
from api import get_bike_station_status
import os
from etl.transform.spark_app import SparkApp
from sqlalchemy import text


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

    def in_pyspark(self, df: pyspark.sql.DataFrame):
        window_spec = Window.partitionBy("zone").orderBy("datetime")

        df = df.withColumn(
            "1h_fwd_apparent_temperature",
            lead("apparent_temperature", 1).over(window_spec),
        )
        df = df.withColumn(
            "1h_fwd_precipitation", lead("precipitation", 1).over(window_spec)
        )

        return df


class StationOccupancyFeatures(DataTransformer):
    def in_pandas(self, df):
        df["pct_full"] = df["full"] / (df["full"] + df["empty"])
        return df

    def in_pyspark(self, df):
        df = df.withColumn("pct_full", df.full / (df.full + df.empty))
        return df


class LagFeatures(DataTransformer):
    def in_pandas(self, df):

        df["30m_blag_pct_full"] = (
            df.sort_values(by="extraction_ts")
            .groupby(by=["station_id"])["pct_full"]
            .transform(lambda x: x.shift(3))  # Assumes each record is 10 mins
        )

        df["120m_avg_pct_full"] = (
            df.sort_values(by="extraction_ts")
            .groupby(by=["station_id"])["pct_full"]
            .transform(
                lambda x: x.rolling(window=12).mean()
            )  # Assumes each record is 10 mins
        )

        return df

    def in_pyspark(self, df):
        windowSpec = Window.partitionBy("station_id").orderBy("extraction_ts")

        df = df.withColumn("30m_blag_pct_full", lag("pct_full", 3).over(windowSpec))

        # Rolling average for 120 minutes (12 periods of 10 minutes)
        df = df.withColumn(
            "120m_avg_pct_full", mean("pct_full").over(windowSpec.rowsBetween(-11, 0))
        )
        return df


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


class FormatToFeaturesSchema(DataTransformer):
    def __init__(self, exec_library):
        super().__init__(exec_library)

    def in_pandas(self, df: pd.DataFrame):
        df = df.set_index(keys=["station_id", "extraction_ts"]).sort_index()
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
            "station_id": np.int64,
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
        youbike_latest = get_bike_station_status.get_bike_station_status(
            extended=True
        ).rename(columns={"updated_at": "extraction_ts"})

        # pull last 120 mins snapshot for lag features
        fresh_extraction_ts = youbike_latest["extraction_ts"][0]
        historic_youbike_data = get_youbike_snapshot_data_for_time_range(
            oldest_ts=fresh_extraction_ts - pd.Timedelta(minutes=120),
            newest_ts=fresh_extraction_ts,
        )

        historic_youbike_data = pd.merge(
            left=historic_youbike_data,
            right=youbike_latest[["id", "weather_zone_id"]],
            on="id",
        )

        concat_youbike = pd.concat(
            [youbike_latest, historic_youbike_data], ignore_index=True
        )

        # Keep requested stations only
        concat_youbike = (
            concat_youbike[concat_youbike["id"].isin(station_ids)]
            .copy(deep=True)
            .rename(columns={"id": "station_id"})
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

        # Some historical stations do no longer exist, based on whether they got a weather_zone_id assigned. If not dropped
        ## Future work is to filter out all stations that do not exist anymore, or at least use them to learn by assigning them to a group
        main_df = main_df.drop(index=main_df[main_df["weather_zone_id"].isna()].index)

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
                    "weather_zone",
                    "1h_fwd_precipitation",
                    "1h_fwd_apparent_temperature",
                ]
            ],
            on=["weather_zone", "y_m_d_h"],
            how="left",
        )
        return main_df

    def in_pyspark(self):
        pass


class CreateInputTrainingFeatures(DataTransformer):
    def in_pandas(self):
        """No implementation in pandas"""
        pass

    def in_pyspark(
        self,
        station_ids: list[int],
        start_period: pd.Timestamp,
        end_period: pd.Timestamp,
    ):
        """
        1. Get historical data for time range
        2. keep only requested stations ids
        3. Get weather zone per station, merge weather zone id to name and merge historical weather data
        4. Validate the features input schema
        """
        spark_app = SparkApp.get_instance()
        spark_session = spark_app.spark_session
        spark_context = spark_app.spark_context
        URI = spark_context._gateway.jvm.java.net.URI
        Path = spark_context._gateway.jvm.org.apache.hadoop.fs.Path
        FileSystem = spark_context._gateway.jvm.org.apache.hadoop.fs.FileSystem

        bucket_name = (
            "stage-youbike" if os.environ["APP_ENV"] == "stage" else "local-youbike"
        )
        bucket_fs = FileSystem.get(
            URI(f"s3a://{bucket_name}/"), spark_app.spark_hadoop_conf
        )

        # Import required data
        hist_weather_reports_uri = [
            obj.getPath().toString()
            for obj in bucket_fs.listStatus(
                Path(f"s3a://{bucket_name}/raw_data/weather/historical_report")
            )
        ]
        hist_weather_report = (
            spark_session.read.option("InferSchema", True)
            .option("header", True)
            .format("parquet")
            .load(hist_weather_reports_uri)
        )
        hist_weather_report = hist_weather_report.dropDuplicates(["datetime", "zone"])

        youbike_snapshot_uri = [
            obj.getPath().toString()
            for obj in bucket_fs.listStatus(Path(f"s3a://{bucket_name}/clean_data/"))
        ][5000:5500]
        hist_snapshot_df = (
            spark_session.read.option("InferSchema", True)
            .option("header", True)
            .format("parquet")
            .load(youbike_snapshot_uri)
        )

        with DB_Connection.from_env().connection as conn:
            station_id_to_weather_id = spark_session.createDataFrame(
                pd.read_sql('SELECT "id", "weather_zone_id"  FROM bike_station;', conn)
            )
            weather_zone_name_df = spark_session.createDataFrame(
                pd.read_sql('SELECT "id", "name" FROM weather_zone;', conn)
            )

        # Only keep requested filters
        hist_snapshot_df = hist_snapshot_df.filter(
            hist_snapshot_df.id.isin(station_ids)
        )

        weather_zone_name_df = weather_zone_name_df.withColumnRenamed(
            "id", "weather_zone_id"
        ).withColumnRenamed("name", "weather_zone")

        main_df = hist_snapshot_df.join(
            station_id_to_weather_id, on="id", how="left"
        ).join(weather_zone_name_df, on="weather_zone_id", how="left")

        # Some historical stations do no longer exist, based on whether they got a weather_zone_id assigned. If not dropped
        ## Future work is to filter out all stations that do not exist anymore, or at least use them to learn by assigning them to a group
        main_df = main_df.filter(~isnan(main_df.weather_zone_id))

        hist_weather_report = MakeWeatherFeatures("pyspark").run(hist_weather_report)
        hist_weather_report = (
            hist_weather_report.withColumn(
                "datetime_unix", hist_weather_report.datetime / 10**9
            )
            .withColumn("datetime", from_unixtime("datetime_unix", "yyyy-MM-dd_HH"))
            .withColumnRenamed("datetime", "y_m_d_h")
            .withColumnRenamed("zone", "weather_zone")
        )

        main_df = main_df.withColumn(
            "y_m_d_h", date_format(main_df.extraction_ts, "yyyy-MM-dd_HH")
        ).withColumnRenamed("id", "station_id")

        main_df = main_df.join(
            hist_weather_report.select(
                [
                    "temperature",
                    "relative_humidity",
                    "apparent_temperature",
                    "precipitation",
                    "wind_speed",
                    "y_m_d_h",
                    "weather_zone",
                    "1h_fwd_precipitation",
                    "1h_fwd_apparent_temperature",
                ]
            ),
            on=["y_m_d_h", "weather_zone"],
            how="left",
        )

        return main_df
