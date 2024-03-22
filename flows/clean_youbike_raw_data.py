import numpy as np
import pandas as pd
from prefect import task, flow
from utils.s3_helper import ConnectionToS3, download_from_bucket
from functools import reduce
from datetime import date, timedelta

class InvalidRecs:
    pass


class TransliterationMapper(dict):
    def __init__(self, dict_mapping, missing_key):
        self.missing_key = missing_key
        self.mapping = dict_mapping

    def __missing__(self, val: str):
        return val


raw_schema = {
    "id": np.int64,
    "name": np.object_,
    "type": np.int64,
    "space": np.int64,
    "full": np.int64,
    "empty": np.int64,
    "bike_yb1": np.int64,
    "bike_yb2": np.int64,
    "bike_eyb": np.int64,
    "city": np.object_,
    "area": np.object_,
    "lat": np.float64,
    "lng": np.float64,
    "place_id": np.float64,
    "address": np.object_,
    "is_open": np.int64,
    "last_update_ts": "datetime64[ns, Asia/Taipei]",  # datetime64 with timezone is not directly represented in numpy
    "extraction_ts": "datetime64[ns, Asia/Taipei]",
}

non_nullable_cols = pd.Index(
    [
        "id",
        "space",
        "full",
        "empty",
        "bike_yb1",
        "bike_yb2",
        "bike_eyb",
        "city",
        "area",
        "lat",
        "lng",
        "last_update_ts",
        "extraction_ts",
    ]
)


def validate_raw_data_schema(df: pd.DataFrame, schema: dict):

    assert df.shape[1] == 18
    for c in df.columns:
        assert df[c].dtype == schema[c]


def validate_values(df: pd.DataFrame, non_nullable_cols: pd.Index):
    cutoff_ts = pd.to_datetime(date.today() - timedelta(days=8)).tz_localize(tz="Asia/Taipei")
    conditions = [
        df[non_nullable_cols].isna().any(axis=1),
        df["type"] != 2,
        df["empty"] < 0,
        df["space"] < 1,  # Stations without space are irrelevant
        df["lat"] < 21.89,  # southermost lat of Taiwan's main island
        df["lng"] < 120,  # westernmost lng of Taiwan's main island
        df["last_update_ts"] < cutoff_ts # earlier updates are considered stale data
    ]

    indice_to_drop = reduce(lambda x, c: x.union(df[c].index), conditions, pd.Index([]))
    df = df.drop(indice_to_drop)
    print(indice_to_drop)
    # Drop out of range values
    return df


def discard_invalid_bike_count(df: pd.DataFrame):
    pass


def map_city(df: pd.DataFrame):
    pass


def map_area(df: pd.DataFrame):
    pass


def format_to_clean_schema(df: pd.DataFrame):
    main_df.drop(labels=["place_id", "address"], axis=1)
    pass


@flow
def clean_youbike_raw_data():
    pass


if __name__ == "__main__":
    # s3_connection = ConnectionToS3.from_env()
    # download_from_bucket(s3_connection, 'raw_data/', 'youbike_dock_info_2024-03-22_13:52:26_raw.parquet', './tmp_data')
    df = pd.read_parquet(
        "./tmp_data/raw_data/youbike_dock_info_2024-03-22_13:52:26_raw.parquet"
    )

    validate_raw_data_schema(df, raw_schema)
    print(validate_values(df, non_nullable_cols).info())
