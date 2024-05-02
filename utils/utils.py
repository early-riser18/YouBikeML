import numpy as np
import pandas as pd
from datetime import datetime
from io import BytesIO
from utils.s3_helper import ConnectionToS3, download_from_bucket, export_file_to_s3
import os
from utils.sql_utils import DB_Connection
from sqlalchemy import text

STANDARD_TS_FORMAT = "%Y-%m-%d_%H:%M:%S"
DB_TS_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_formatted_timestamp_as_str(ts: datetime) -> str:

    return f"{ts:%Y-%m-%d_%H:%M:%S}"


def get_youbike_snapshot_data_for_time_range(
    oldest_ts: pd.Timestamp, newest_ts: pd.Timestamp
) -> pd.DataFrame:
    """Retrieve the snapshots for the range defined. For prediction, range is only last 120 minutes.
    For training, since use pyspark, probably cannot use this method.
    """
    s3 = ConnectionToS3.from_env()
    bucket = s3.resource.Bucket(s3.bucket_name)

    snapshot_files_by_key = sorted(
        [
            obj.key
            for obj in bucket.objects.filter(Prefix="clean_data/youbike_dock_info_2")
        ],
        reverse=True,
    )

    def find_file_index_by_ts(ts: str) -> int:
        i = 0
        while snapshot_files_by_key[i] > f"clean_data/youbike_dock_info_{ts}":
            i += 1
        return i

    oldest_i = find_file_index_by_ts(oldest_ts.strftime(STANDARD_TS_FORMAT))
    newest_i = find_file_index_by_ts(newest_ts.strftime(STANDARD_TS_FORMAT))

    def read_parquet_from_s3(bucket, key):
        """Generator function to yield a DataFrame from S3"""
        s3_res = bucket.Object(key).get()
        yield pd.read_parquet(BytesIO(s3_res["Body"].read()))

    dfs = (
        df
        for key in snapshot_files_by_key[newest_i:oldest_i]
        for df in read_parquet_from_s3(bucket, key)
    )
    hist_df = pd.concat(dfs, ignore_index=True)
    return hist_df


def get_latest_weather_data() -> pd.DataFrame:
    s3 = ConnectionToS3.from_env()
    bucket = s3.resource.Bucket(s3.bucket_name)
    weather_files_by_key = [
        obj.key
        for obj in bucket.objects.filter(
            Prefix="raw_data/weather/forecast_report/weather_forecast_report_"
        )
    ]
    latest_report_key = sorted(weather_files_by_key, reverse=True)[0]
    report_local_path = download_from_bucket(
        s3, latest_report_key, "/tmp", preserve_path=False
    )
    return pd.read_parquet(report_local_path)


def get_weather_zone() -> pd.DataFrame:
    """Retrieve weather zones dims from db's weather_zone"""
    with DB_Connection.from_env().connection as conn:
        cursor_result = conn.execute(text("SELECT * FROM weather_zone;"))
        weather_zone_df = pd.DataFrame(cursor_result.all())
    return weather_zone_df


def np_magnitude(coord1: pd.DataFrame, coord2: pd.DataFrame):
    lat_vector = np.power(coord1["lat"] - coord2["lat"], 2)
    lng_vector = np.power(coord1["lng"] - coord2["lng"], 2)
    return np.sqrt(lat_vector + lng_vector)


def identify_weather_zone(lat: pd.Series, lng: pd.Series) -> pd.Series:
    """
    Provided a dataframe with column lat and column lng, identify what the closest weather zone is.
    """
    weather_zones = get_weather_zone()

    mag = pd.DataFrame({"lat": lat, "lng": lng}).loc[:]

    for i, r in weather_zones.iterrows():
        mag[r["name"]] = np_magnitude(
            pd.DataFrame({"lat": lat, "lng": lng}),
            weather_zones[weather_zones["name"] == r["name"]].iloc[0],
        )
    return mag.drop(["lat", "lng"], axis=1).idxmin(axis=1)


if __name__ == "__main__":
    import sys

    # print(
    #     get_youbike_snapshot_data_for_time_range(
    #         oldest_ts=pd.Timestamp("2024-03-28-14:28"),
    #         newest_ts=pd.Timestamp("2024-03-28-15:32"),
    #     )
    # )
