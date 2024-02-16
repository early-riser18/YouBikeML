import openmeteo_requests
from retry_requests import retry
import pandas as pd
from datetime import datetime
import pytz
from utils.utils import get_formatted_timestamp_as_str

class WeatherSnapshot:
	def __init__(self, extraction_ts: datetime, body: pd.DataFrame):
		self.extraction_ts = extraction_ts
		self.body = body


def get_weather_data() -> WeatherSnapshot:
	"""Returns the requested data from the weather API"""
	retry_session = retry(retries=5, backoff_factor=0.2)
	openmeteo = openmeteo_requests.Client(session=retry_session)

	url = "https://api.open-meteo.com/v1/forecast"
	requested_fields = [
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
	params = {
		"latitude": 25.0478,
		"longitude": 121.5319,
		"hourly": requested_fields,
		"timeformat": "unixtime",
		"timezone": "Asia/Singapore",
		"past_days": 1,
		"forecast_days": 2,
	}
	
	responses = openmeteo.weather_api(url, params=params)
	hourly = responses[0].Hourly()
	data_dic = {}
	for i in requested_fields:
		current_index = requested_fields.index(i)
		data_dic[i] = hourly.Variables(current_index).ValuesAsNumpy()
	data_dic["date"] = (
		pd.date_range(
			start=pd.to_datetime(hourly.Time(), unit="s"),
			end=pd.to_datetime(hourly.TimeEnd(), unit="s"),
			freq=pd.Timedelta(seconds=hourly.Interval()),
			inclusive="left",
		)
		.tz_localize("UTC")
		.tz_convert("Asia/Taipei")
	)
	tz_tst = pytz.timezone("Asia/Taipei")
	time_now = datetime.today().now(tz=tz_tst)
	weather = WeatherSnapshot(extraction_ts=time_now, body=pd.DataFrame(data_dic))
	return weather

def basic_preprocessing(data: any) -> WeatherSnapshot:
	"""
	Constructs a WeatherSnapshot object from the API native response object WeatherApiResponse 
	"""
	pass

def persist_data(data: WeatherSnapshot) -> str:
	# TODO: Logic for writing depending on env.
	formated_ts = get_formatted_timestamp_as_str(data.extraction_ts)
	file_path = f"./raw_data/weather_data_raw_{formated_ts}.csv"
	data.body.to_csv(file_path, index=False)
	return file_path


if __name__ == "__main__":
	weather = get_weather_data()
	persist_data(weather)