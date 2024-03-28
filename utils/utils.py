from datetime import datetime


def get_formatted_timestamp_as_str(ts: datetime) -> str:

    return f"{ts:%Y-%m-%d_%H:%M:%S}"

def get_weather_zones() -> pd.DataFrame:
    """Retrieve weather zones dims from s3 bucket"""
    s3 = ConnectionToS3.from_env()
    bucket = s3.resource.Bucket(s3.bucket_name)
    key = "raw_data/weather/weather_zones.parquet"
    s3_res = bucket.Object(key).get()
    return pd.read_parquet(BytesIO(s3_res["Body"].read()))



def np_magnitude(coord1: pd.DataFrame, coord2: pd.DataFrame):
    lat_vector = np.power(coord1["lat"] - coord2["lat"], 2)
    lng_vector = np.power(coord1["lng"] - coord2["lng"], 2)
    return np.sqrt(lat_vector + lng_vector)


def identify_weather_zone(lat: pd.Series, lng: pd.Series) -> pd.Series:
    """
    Provided a dataframe with column lat and column lng, identify what the closest weather zone is.
    """
    weather_zones = get_weather_zones()

    mag = pd.DataFrame({"lat": lat, "lng": lng}).loc[:]

    for i, r in weather_zones.iterrows():
        mag[r["name"]] = np_magnitude(
            pd.DataFrame({"lat": lat, "lng": lng}),
            weather_zones[weather_zones["name"] == r["name"]].iloc[0],
        )
    return mag.drop(["lat", "lng"], axis=1).idxmin(axis=1)


if __name__ == "__main__":
    print(get_formatted_timestamp_as_str(datetime.now()))
