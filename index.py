import streamlit as st
import pandas as pd
import numpy as np
from etl.extraction.youbike import extract_youbike_raw_data
from etl.transform.clean_youbike_data import clean_youbike_data
from etl.transform.features_lib import StationOccupancyFeatures
from api.forecast_service import  get_fill_rate_forecast
import requests
import json


st.set_page_config(page_title="YouBike Forecast", page_icon="🚲", layout="wide")

youbike_base_table_schema = ["station_id", "name", "full", "pct_full"] 
youbike_table_prediction_schema = [
    "station_id",
    "name",
    "full",
    "fill_rate",
    "fill_rate_t+1",
]
youbike_shortage_schema = ["station_id", "name", "space", "avg_fill_rate"]
col_mapping = {
    "station_id": "Station #",
    "area": "Area",
    "name": "Name",
    "full": "Available Bikes",
    "pct_full": "Fill Level",
    "extraction_ts": "Last Refreshed",
    "fill_rate": "Fill Level",
    "fill_rate_t+1": "Fill Level in 30m",
    "avg_fill_rate": "Avg. Fill Level Predicted",
    "space": "Total Bike Slots",
}

col_format = {
    "station_id": "{:}",
    "pct_full": "{:.0%}",
    "fill_rate": "{:.0%}",
    "fill_rate_t+1": "{:.0%}",
    "avg_fill_rate": "{:.0%}",
}


def create_view(
    df: pd.DataFrame, cols: list[str], col_rename: dict = None, col_format: dict = None
):
    """Provided a dataframe, a list of columns, a column name mapping and display format, returns a view of that dataframe"""
    out_df = df[cols].copy()

    if col_format:
        for col_n, fmt in col_format.items():
            try:
                out_df[col_n] = out_df[col_n].transform(lambda x: fmt.format(x))
            except Exception as e:
                print(f"Unable to format {col_n}: {e}")

    if col_rename:
        out_df.rename(columns=col_rename, inplace=True)

    out_df = out_df.style.apply(styling_color_rows, axis=0)
    return out_df



@st.cache_data(ttl=180)
def get_forecast(stations_id: list[int]):
    forecast_df = get_fill_rate_forecast(stations_id)
    return forecast_df


@st.cache_data(ttl=180)
def get_youbike_snapshot():
    """Pull latest snapshot of the youbikes"""
    youbike_snapshot = extract_youbike_raw_data()
    return clean_youbike_data(youbike_snapshot.body)


def get_average_forecast_table(df, is_ascending: bool):
    try:
        df["avg_fill_rate"] = (df["fill_rate"] + df["fill_rate_t+1"]) / 2
        df_1 = (
            df.sort_values(by="avg_fill_rate", ascending=is_ascending)
            .reset_index(drop=True)
            .head(10)
        )
        return create_view(df_1, youbike_shortage_schema, col_mapping, col_format)
    except Exception as e:
        return "*No forecast available yet.*"


def styling_color_rows(row):
    return [
        "background-color: #f7f9ff" if i % 2 == 0 else "background-color: white"
        for i in range(len(row))
    ]


base_youbike_df = get_youbike_snapshot().rename(columns={"id": "station_id"})
youbike_pull_ts = base_youbike_df["extraction_ts"].iloc[0]
base_youbike_df["pct_full"] = StationOccupancyFeatures("pandas").run(base_youbike_df)[
    "pct_full"
]

### HEADER ###
banner = st.container()
banner.image("webapp/youbike_logo.png", width=150)

### PART 1 ###
c = st.container()
c.header("Predict the demand for YouBikes in Taiwan today")
col1, col2, col3, col4 = c.columns(4)

selected_city = col1.selectbox(
    "Select a city", options=base_youbike_df["city"].unique()
)

col2.write("")
col2.write("")
clicked = col2.button("**Forecast Stations Fill Level**", type="primary")
c.divider()
c.subheader("Overview")
c.markdown(f"Last refreshed: {youbike_pull_ts}")


filtered_table = (
    base_youbike_df[base_youbike_df["city"] == selected_city]
    .reset_index(drop=True)
    .copy(deep=True)
)

if clicked == 1:
    station_ids_filtered = base_youbike_df[base_youbike_df["city"] == selected_city][
        "station_id"
    ].unique()

    try: 
        # raw_res = get_forecast(stations_id=station_ids_filtered.tolist())
        forecast_raw = get_forecast(stations_id=station_ids_filtered.tolist())
        forecast = forecast_raw[forecast_raw["relative_ts"] == 0].copy(deep=True)
        for i in forecast_raw["relative_ts"].unique()[1:]:
            forecast = pd.merge(
                left=forecast,
                right=forecast_raw[forecast_raw["relative_ts"] == i][
                    ["station_id", "fill_rate", "base_ts"]
                ],
                on="station_id",
                suffixes=["", f"_t+{i}"],
            )
        main_table = pd.merge(left=filtered_table, right=forecast, on="station_id")


        c.write(
            create_view(
                main_table, youbike_table_prediction_schema, col_mapping, col_format
            )
        )

    except Exception as e:
        raise e
        c.write("An error occured. Please try again later. 😔")
        main_table = filtered_table
else:
    main_table = filtered_table
    c.write(create_view(main_table, youbike_base_table_schema, col_mapping, col_format))

### PART 2 ###
col_b1, col_b2 = c.columns(2)

col_b1.subheader("Stations with the highest risk of a bike shortage")
col_b1.write("Average fill level in the next 30 minutes.")
highest_shortage = get_average_forecast_table(main_table, True)
col_b1.write(highest_shortage)

col_b2.subheader("Stations with the lowest risk of a bike shortage")
col_b2.write("Average fill level in the next 30 minutes.")
lowest_shortage = get_average_forecast_table(main_table, False)
col_b2.write(lowest_shortage)

st.write("")
st.write("")
st.write("")
st.write(
    "*Disclaimer: This service is not affiliated with YouBike Co., Ltd.*<br>Learn more about this project on [Github](https://github.com/early-riser18/youbike).",
    unsafe_allow_html=True,
)
st.write("  ")