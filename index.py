import streamlit as st
import pandas as pd
import numpy as np
from extraction.youbike import extract_youbike_raw_data
from transform.clean_youbike_data import clean_youbike_data
from transform.features_lib import StationOccupancyFeatures
import requests
import json


st.set_page_config(page_title="YouBike Forecast", page_icon="🚲", layout="wide")

youbike_base_table_schema = ["id", "name", "full", "pct_full"]
youbike_table_prediction_schema = [
    "id",
    "name",
    "full",
    "occupancy_lvl",
    "occupancy_lvl_t+1",
]
youbike_shortage_schema = ["id", "name", "space", "avg_occupancy"]
col_mapping = {
    "id": "Station #",
    "area": "Area",
    "name": "Name",
    "full": "Available Bikes",
    "pct_full": "Fill Level",
    "extraction_ts": "Last Refreshed",
    "occupancy_lvl": "Fill Level",
    "occupancy_lvl_t+1": "Fill Level in 30m",
    "avg_occupancy": "Avg. Fill Level Predicted",
    "space": "Bike Slots",
}

col_format = {
    "id": "{:}",
    "pct_full": "{:.0%}",
    "occupancy_lvl": "{:.0%}",
    "occupancy_lvl_t+1": "{:.0%}",
    "avg_occupancy": "{:.0%}",
}


def create_view(
    df: pd.DataFrame, cols: list[str], col_rename: dict = None, col_format: dict = None
):
    """Provided a dataframe, a list of columns and a column name mapping, returns a view of that dataframe"""
    out_df = df[cols]

    if col_format:
        for col_n, fmt in col_format.items():
            try:
                out_df[col_n] = out_df[col_n].transform(lambda x: fmt.format(x))
            except:
                print(f"Unable to format {col_n}")

    if col_rename:
        out_df.rename(columns=col_rename, inplace=True)

    out_df = out_df.style.apply(styling_color_rows, axis=0)
    return out_df


@st.cache_data(ttl=300)
def warm_up_lambda():
    get_forecast([508201032])


@st.cache_data(ttl=180)
def get_forecast(stations_id: list[int]):
    url = "https://33iqhftc6fakzdlczkjsiikhtu0odrec.lambda-url.ap-northeast-1.on.aws/"
    payload = {"station_id": stations_id}
    res = requests.post(url, json=payload)
    return res.text


@st.cache_data(ttl=180)
def get_youbike_snapshot():
    youbike_snapshot = extract_youbike_raw_data()
    return clean_youbike_data(youbike_snapshot.body)


def get_average_forecast_table(df, is_ascending: bool):
    try:
        df["avg_occupancy"] = (df["occupancy_lvl"] + df["occupancy_lvl_t+1"]) / 2
        df_1 = (
            df.sort_values(by="avg_occupancy", ascending=is_ascending)
            .reset_index(drop=True)
            .head(10)
        )
        return create_view(df_1, youbike_shortage_schema, col_mapping, col_format)
    except:
        return "*No forecast available yet.*"

def styling_color_rows(row):
    return ['background-color: #f7f9ff' if i % 2 == 0 else 'background-color: white' for i in range(len(row))]

    


base_youbike_df = get_youbike_snapshot()
youbike_pull_ts = get_youbike_snapshot()["extraction_ts"].iloc[0]
base_youbike_df["pct_full"] = StationOccupancyFeatures("pandas").run(base_youbike_df)[
    "pct_full"
]
banner = st.container()
banner.image('webapp/youbike_logo.png',width=150)
c = st.container()
c.header("Predict the demand for YouBikes in Taiwan today")
col1, col2, col3, col4 = c.columns(4)


selected_city = col1.selectbox(
    "Select a city", options=base_youbike_df["city"].unique()
)
filtered_table = (
    base_youbike_df[base_youbike_df["city"] == selected_city]
    .reset_index(drop=True)
    .copy(deep=True)
)
col2.write("")
col2.write("")
clicked = col2.button("**Forecast Stations Fill Level**", type="primary")
c.divider()
c.subheader("Overview")
c.markdown(f"Last refreshed: {youbike_pull_ts}")


if clicked == 1:
    station_ids_filtered = base_youbike_df[base_youbike_df["city"] == selected_city][
        "id"
    ].unique()
    print(station_ids_filtered.tolist())
    raw_res = get_forecast(stations_id=station_ids_filtered.tolist())

    forecast_raw = pd.DataFrame(json.loads(raw_res))
    forecast = forecast_raw[forecast_raw["relative_ts"] == 0].copy(deep=True)
    for i in forecast_raw["relative_ts"].unique()[1:]:
        forecast = pd.merge(
            left=forecast,
            right=forecast_raw[forecast_raw["relative_ts"] == i][
                ["id", "occupancy_lvl", "ts"]
            ],
            on="id",
            suffixes=["", f"_t+{i}"],
        )

    main_table = pd.merge(left=filtered_table, right=forecast, on="id")
    c.write(
        create_view(
            main_table, youbike_table_prediction_schema, col_mapping, col_format
        )
    )
else:
    main_table = filtered_table
    c.write(create_view(main_table, youbike_base_table_schema, col_mapping, col_format))


col_b1, col_b2 = c.columns(2)

col_b1.subheader("Stations with the highest risk of a bike shortage")
col_b1.write("Average fill level in the next 30 minutes.")
highest_shortage = get_average_forecast_table(main_table, True)
col_b1.write(highest_shortage)
# col_b1.markdown(f"Last refreshed: {youbike_pull_ts}")

col_b2.subheader("Stations with the lowest risk of a bike shortage")
col_b2.write("Average fill level in the next 30 minutes.")
lowest_shortage = get_average_forecast_table(main_table, False)
col_b2.write(lowest_shortage)
# col_b2.markdown(f"Last refreshed: {youbike_pull_ts}")

st.write("")
st.write("")
st.write("")
st.write("*Disclaimer: This service is not affiliated with YouBike Co., Ltd.*<br>Learn more about this project on [Github](https://github.com/early-riser18/youbike).", unsafe_allow_html=True)
st.write("  ")

warm_up_lambda()
