from utils.utils import get_formatted_timestamp_as_str, STANDARD_TS_FORMAT
from utils.s3_helper import ConnectionToS3, export_file_to_s3, download_from_bucket
from extraction.youbike import extract_youbike_raw_data
from transform.clean_youbike_data import clean_youbike_data
from prefect import flow
from prefect.deployments import Deployment
import pandas as pd
from io import BytesIO


def rerun_ingest_youbike_to_clean_layer(
    oldest_ts: pd.Timestamp, newest_ts: pd.Timestamp
):
    """ """
    s3 = ConnectionToS3.from_env()
    bucket = s3.resource.Bucket(s3.bucket_name)
    print(
        f"Attempting rerun youbike ingestion to clean layer from {oldest_ts} to {newest_ts}"
    )
    snapshot_files_by_key = sorted(
        [
            obj.key
            for obj in bucket.objects.filter(Prefix="raw_data/youbike_dock_info_2")
        ],
        reverse=False,
    )
    def find_file_index_by_ts(ts: str) -> int:
        i = 0
        while snapshot_files_by_key[i] < f"raw_data/youbike_dock_info_{ts}_raw.parquet":
            i += 1
        return i

    oldest_i = find_file_index_by_ts(oldest_ts.strftime(STANDARD_TS_FORMAT))
    newest_i = find_file_index_by_ts(newest_ts.strftime(STANDARD_TS_FORMAT))

    print(oldest_i, newest_i)

    file_ext = "parquet"

    for key in snapshot_files_by_key[oldest_i:newest_i]:
        s3_res = bucket.Object(key).get()
        snapshot_df = pd.read_parquet(BytesIO(s3_res["Body"].read()))
        run_ts = key.split("raw_data/youbike_dock_info_")[1].split("_raw")[0]
        file_stub = f"youbike_dock_info_{run_ts}"

        clean_youbike_df = clean_youbike_data(snapshot_df)

        clean_upload_uri = export_file_to_s3(
            connection=s3,
            file_name=f"clean_data/{file_stub}.{file_ext}",
            body=clean_youbike_df.to_parquet(index=False),
        )
        print("Clean data uploaded at: ", clean_upload_uri)


if __name__ == "__main__":
    rerun_ingest_youbike_to_clean_layer(
        pd.Timestamp("2024-04-03 15:00"), pd.Timestamp("2024-04-03 17:10")
    )
