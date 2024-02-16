from .youbike import	download_basic_preprocessed_youbike_snapshot
from time import sleep
from datetime import datetime

while True:
	print(f"{datetime.now()} Downloading...")
	path = download_basic_preprocessed_youbike_snapshot()
	print(f"{datetime.now()} Downloaded at ", path)
	print(f"{datetime.now()} Sleep for 5 mins.")
	sleep(300)