from sqlalchemy import text
import pandas as pd
from utils import sql_utils, utils
from datetime import datetime
import pytz


def db_update_bike_station_status(df: pd.DataFrame):
    """Upsert to the bike_station_status table in the database
    Expects a clean_youbike_data schema
    """
    df["extraction_ts"] = df["extraction_ts"].dt.strftime(utils.DB_TS_FORMAT)
    df.rename(columns={"extraction_ts": "updated_at"}, inplace=True)
    columns = ["id", "full", "empty", "updated_at"]

    statement = sql_utils.SQL_INSERT_STATEMENT_FROM_DATAFRAME(
        df[columns], "bike_station_status"
    )
    upsert_sql = ' ON CONFLICT ("id") DO UPDATE SET "full" = EXCLUDED."full", "empty" = EXCLUDED."empty", "updated_at" = EXCLUDED."updated_at"'
    statement = statement.split(";")[0] + upsert_sql + ";"

    with sql_utils.DB_Connection.from_env()._connection as conn:
        conn.execute(text(statement))
        conn.commit()


def db_update_bike_station(df: pd.DataFrame) -> None:
    """
    Insert new bike stations found in source sysstem
    Expects a clean_youbike_data schema
    """
    incoming_ids = df["id"].tolist()
    with sql_utils.DB_Connection.from_env().connection as conn:
        existing_ids = [
            row[0] for row in conn.execute(text('SELECT "id" from bike_station;')).all()
        ]
    new_ids = set(incoming_ids) - set(existing_ids)

    if len(new_ids) > 0:
        new_stations = df[df["id"].isin(new_ids)].copy()
        new_stations["weather_zone"] = utils.identify_weather_zone(
            new_stations["lat"], new_stations["lng"]
        )
        weather_zones = utils.get_weather_zone()
        new_stations = pd.merge(
            left=new_stations,
            right=weather_zones[["id", "name"]],
            how="left",
            left_on="weather_zone",
            right_on="name",
            suffixes=["", "_weather_zone"],
        )
        new_stations["created_at"] = datetime.today().strftime(utils.DB_TS_FORMAT)
        new_stations.rename(
            columns={"id_weather_zone": "weather_zone_id"}, inplace=True
        )
        sql_insert = sql_utils.SQL_INSERT_STATEMENT_FROM_DATAFRAME(
            new_stations[
                [
                    "id",
                    "lat",
                    "lng",
                    "city",
                    "name",
                    "area",
                    "weather_zone_id",
                    "created_at",
                ]
            ],
            "bike_station",
        )

        with sql_utils.DB_Connection.from_env().connection as conn:
            conn.execute(text(sql_insert))
            conn.commit()


def get_all_valid_stations_id() -> list[int]:
    """Returns a list with all station ids present in the db.bike_station table. Currently no validity filter is checked."""
    sql_text = 'SELECT "id" FROM bike_station;'
    with sql_utils.DB_Connection.from_env().connection as conn:
        res = conn.execute(text(sql_text))
        return [r[0] for r in res.all()]


if __name__ == "__main__":
    print(get_all_valid_stations_id())
