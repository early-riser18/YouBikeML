from datetime import datetime

def get_formatted_timestamp_as_str(ts: datetime) -> str:

	return f"{ts:%Y-%m-%d_%H:%M:%S}"


if __name__ == "__main__":
	print(get_formatted_timestamp_as_str(datetime.now()))