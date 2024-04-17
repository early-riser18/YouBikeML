import numpy as np
import pandas as pd
from prefect import task, flow
from utils.s3_helper import ConnectionToS3, download_from_bucket

# from utils.s3_helper import ConnectionToS3, download_from_bucket
from functools import reduce
from datetime import date, timedelta
from transform.youbike_mapping import YOUBIKE_AREA_MAPPING, YOUBIKE_CITY_MAPPING


class InvalidRecs:
    pass


class TransliterationMapper(dict):
    def __init__(self, dict_mapping, missing_key="N/A"):
        super().__init__(dict_mapping)
        self.missing_key = missing_key

    def __missing__(self, key):
        return self.missing_key


RAW_SCHEMA = {
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
    "last_update_ts": pd.DatetimeTZDtype(unit="ms", tz="Asia/Taipei"),
    "extraction_ts": pd.DatetimeTZDtype(unit="ms", tz="Asia/Taipei"),
}

OUTPUT_SCHEMA = {
    "id": np.int64,
    "name": np.object_,
    "lat": np.float64,
    "lng": np.float64,
    "space": np.int64,
    "full": np.int64,
    "empty": np.int64,
    "bike_yb2": np.int64,
    "bike_eyb": np.int64,
    "city": np.object_,
    "area": np.object_,
    "last_update_ts": pd.DatetimeTZDtype(unit="ms", tz="Asia/Taipei"),
    "extraction_ts": pd.DatetimeTZDtype(unit="ms", tz="Asia/Taipei"),
}
non_nullable_cols = pd.Index(
    [
        "id",
        "space",
        "full",
        "empty",
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


def validate_data_schema(
    df: pd.DataFrame,
    schema: dict,
    col_nbr: int,
) -> bool:
    print("validation: ", df.shape)
    try:
        assert df.shape[1] == col_nbr
        for c in df.columns:
            assert df[c].dtype == schema[c]
            print(f"{c} as {df[c].dtype}: OK")

    except:
        return False
    return True


def validate_values(df: pd.DataFrame, non_nullable_cols: pd.Index) -> pd.DataFrame:
    cutoff_ts = pd.to_datetime(date.today() - timedelta(days=8)).tz_localize(
        tz="Asia/Taipei"
    )
    conditions = [
        df[non_nullable_cols].isna().any(axis=1),
        df["type"] != 2,  # Youbike 1.0 are soon deprecrated
        df["empty"] < 0,  # Empty space cannot be negative
        df["space"] < 1,  # Stations without space are irrelevant
        df["lat"] < 21.89,  # southermost lat of Taiwan's main island
        df["lng"] < 120,  # westernmost lng of Taiwan's main island
        df["last_update_ts"] < cutoff_ts,  # earlier updates are considered stale data,
        df["space"]
        < (
            df["full"] + df["empty"] - 10
        ),  # 10 is arbitrary threshold. Needs further investigation.
    ]
    indice_to_drop = reduce(lambda x, c: x.union(df[c].index), conditions, pd.Index([]))
    df = df.drop(indice_to_drop)
    return df


def handle_duplicates(df: pd.DataFrame):

    df = df.drop_duplicates(
        subset=[
            "id",
            "space",
            "full",
            "empty",
            "bike_yb2",
            "bike_eyb",
            "city",
            "area",
            "lat",
            "lng",
            "is_open",
            "last_update_ts",
            "extraction_ts",
        ],
        ignore_index=True,
    )

    df["pos"] = list(zip(df["lat"], df["lng"]))
    df["duplic_pos"] = df.groupby(["pos", "extraction_ts"]).transform("size")
    df = df.drop(df[df["duplic_pos"] > 1].index)
    df = df.drop(["pos", "duplic_pos"], axis=1)
    return df


def format_to_clean_schema(df: pd.DataFrame):
    df = (
        df[OUTPUT_SCHEMA.keys()]
        .sort_values(by=["id", "extraction_ts"])
        .reset_index(drop=True)
    )
    return df


def validate_output_schema(df: pd.DataFrame, schema: dict):
    try:
        assert df.shape[1] == 13
        for c in df.columns:
            assert df[c].dtype == schema[c]
    except:
        return False
    return True


@flow
def clean_youbike_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans raw youbike data and returns it.
    """
    if validate_data_schema(df, RAW_SCHEMA, 18) == False:
        print(df.head(10), df.info())
        raise AssertionError(f"Failed raw schema validation \n")

    df = validate_values(df, non_nullable_cols)
    df = handle_duplicates(df)
    city_name_mapper = TransliterationMapper(YOUBIKE_CITY_MAPPING)
    area_name_mapper = TransliterationMapper(YOUBIKE_AREA_MAPPING)
    df["city"] = df["city"].map(city_name_mapper)
    df["area"] = df["area"].map(area_name_mapper)
    df = format_to_clean_schema(df)

    if validate_data_schema(df, RAW_SCHEMA, 13) == False:
        print(df.head(10), df.info())
        raise AssertionError(f"Failed clean schema validation \n")
    return df


if __name__ == "__main__":
    stub = "youbike_dock_info_2024-04-17_09:01:29"
    s3_connection = ConnectionToS3.from_env()
    download_from_bucket(
        s3_connection,
        f"raw_data/{stub}",
        "./tmp/",
       True
    )
    df = pd.read_parquet(
        f"./tmp/raw_data/{stub}_raw.parquet"
    )

    TIMEZONE = "Asia/Taipei"
    df["last_update_ts"] = df["last_update_ts"].astype(f"datetime64[ms, {TIMEZONE}]")
    df["extraction_ts"] = df["extraction_ts"].astype(f"datetime64[ms, {TIMEZONE}]")

    clean_df = clean_youbike_data(df)
    clean_df.to_parquet(f"./tmp/{stub}_clean.parquet")
