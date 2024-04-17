import requests
import pandas as pd
from datetime import datetime
import pytz
from io import StringIO
from prefect import task, flow

TIMEZONE = "Asia/Taipei"


class YoubikeSnapshot:
    def __init__(self, extraction_ts: datetime, body: pd.DataFrame):
        self.extraction_ts = extraction_ts
        self.body = body



def get_youbike_data() -> YoubikeSnapshot:
    """
    Returns the requested data from the youbike endpoint.

        Parameters:
            None -- Endpoint is a constant

        Returns:
            YoubikeSnapshot -- Object containing the requested data
    """

    URL = "https://gcs-youbike2-linebot.microprogram.tw/latest-data/youbike-station.csv"
    r = requests.request("GET", URL)
    if r.status_code != 200:
        raise Exception(f"API call to {URL} failed.")
    tz_tst = pytz.timezone(TIMEZONE)
    time_now = datetime.today().now(tz=tz_tst)
    res_as_df = pd.read_csv(StringIO(r.text))
    data = YoubikeSnapshot(extraction_ts=time_now, body=res_as_df)
    return data



def basic_preprocessing(data: YoubikeSnapshot) -> YoubikeSnapshot:
    """
    Applies simple preprocessing.

        Parameters:
            data (YoubikeSnapshot) -- The raw snapshot to preprocess

        Returns:
            YoubikeSnapshot -- The preprocessed snapshot
    """

    df = data.body
    df["last_update_ts"] = (
        pd.to_datetime(df["updated_at"], unit="s")
        .dt.tz_localize(tz="UTC")
        .dt.tz_convert(tz=TIMEZONE)
    )
    df["extraction_ts"] = pd.to_datetime(data.extraction_ts.replace(microsecond=0)).tz_convert(tz=TIMEZONE)
    df["last_update_ts"] = df["last_update_ts"].astype(f"datetime64[ms, {TIMEZONE}]")
    df["extraction_ts"] = df["extraction_ts"].astype(f"datetime64[ms, {TIMEZONE}]")

    df.drop(labels=["updated_at"], axis=1, inplace=True)
    data.body = df
    return data



def extract_youbike_raw_data() -> YoubikeSnapshot:
    """
    Retrieve Youbike snapshot from endpoint, preprocess it and persist it.

        Parameters:
            None

        Returns:
            YouBikeSnapshot -- Object with the extracted data and metadata
    """
    data = get_youbike_data()
    preprocessed_data = basic_preprocessing(data)
    return preprocessed_data
