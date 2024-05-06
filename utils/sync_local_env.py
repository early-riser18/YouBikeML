import os
from utils.s3_helper import ConnectionToS3, export_file_to_s3


def sync_local_from_stage(filter: str, limit: int = 9999):

    curr_env = os.environ["APP_ENV"]
    os.environ["APP_ENV"] = "local"
    local_co = ConnectionToS3.from_env()
    os.environ["APP_ENV"] = "stage"
    stage_co = ConnectionToS3.from_env()
    os.environ["APP_ENV"] = curr_env

    key_to_dl = sorted(
        [obj.key for obj in stage_co.Bucket.objects.filter(Prefix=filter)],
        reverse=True,
    )

    key_in_storage = local_co.Bucket.objects.filter(Prefix=filter)
    # download and upload
    for key in key_to_dl[:limit]:
        if key not in [obj.key for obj in key_in_storage]:
            res = stage_co.Bucket.Object(key).get()
            export_file_to_s3(local_co, key, res["Body"].read())
            print(f"uploaded {key} at {local_co.bucket_name}")
        else:
            print(f"{key} already downloaded. Skipping it.")


if __name__ == "__main__":
    sync_local_from_stage("raw_data/youbike_dock_info_202", limit=200)
    sync_local_from_stage("clean_data/youbike_dock_info_202", limit=200)
    sync_local_from_stage("raw_data/weather/forecast_report", limit=1)
    sync_local_from_stage("raw_data/weather/forecast_report", limit=1)
    sync_local_from_stage("model", limit=1)
    