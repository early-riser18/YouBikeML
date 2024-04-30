from utils.utils import DB_Connection
from sqlalchemy import text
from datetime import datetime, timedelta
import pandas as pd
from etl.extraction import youbike
from etl.transform import clean_youbike_data
from db import main


def get_bike_station_status(extended: bool = False):

    # Check if DB has already fresh data - if no refresh, update and serve
    MIN_UNTIL_REFRESH = 5
    query = "SELECT * from bike_station_status bss;"

    if extended:
        query = query.split(";")[0] + " JOIN bike_station bs ON bs.id = bss.id;"

    with DB_Connection.from_env().connection as conn:
        bike_station_status_rows = conn.execute(text(query)).all()

        # Assumes table is always recreated on refresh
        if bike_station_status_rows[0][3] < (
            datetime.now() - timedelta(minutes=MIN_UNTIL_REFRESH)
        ):
            print(
                f"bike_station_status last refresh more than {MIN_UNTIL_REFRESH} ago. Refreshing..."
            )
            raw_youbike = youbike.extract_youbike_raw_data()
            clean_youbike = clean_youbike_data.clean_youbike_data(raw_youbike.body)
            main.db_update_bike_station_status(clean_youbike)

            bike_station_status_rows = conn.execute(text(query)).all()

    bike_station_status_df = pd.DataFrame(bike_station_status_rows)
    bike_station_status_df = bike_station_status_df.iloc[
        :, ~bike_station_status_df.columns.duplicated()
    ]

    return bike_station_status_df


if __name__ == "__main__":
    test_station_ids = [
        508201032,
        501208101,
        501216049,
        501210126,
        501209089,
        508201041,
    ]
    # print(get_bike_station_status(extended=True))
