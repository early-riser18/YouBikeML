from utils.utils import get_formatted_timestamp_as_str
from utils.s3_helper import ConnectionToS3, export_file_to_s3
from extraction.youbike import extract_youbike_raw_data
from transform.clean_youbike_data import clean_youbike_data
from prefect import flow
from prefect.deployments import Deployment


@flow(log_prints=True)
def youbike_snapshots_ingestion():
    """ """
    s3_co = ConnectionToS3.from_env()
    youbike_snapshot = extract_youbike_raw_data()

    run_ts = get_formatted_timestamp_as_str(youbike_snapshot.extraction_ts)
    file_stub = f"youbike_dock_info_{run_ts}"
    file_ext = "parquet"

    raw_upload_file_path = f"raw_data/{file_stub}_raw.{file_ext}"
    raw_df_as_parquet = youbike_snapshot.body.to_parquet(index=False)
    raw_upload_uri = export_file_to_s3(
        connection=s3_co, file_name=raw_upload_file_path, body=raw_df_as_parquet
    )
    print("Raw data uploded to: ", raw_upload_uri)

    clean_youbike_df = clean_youbike_data(youbike_snapshot.body)
    clean_upload_file_path = f"clean_data/{file_stub}.{file_ext}"
    clean_df_as_parquet = clean_youbike_df.to_parquet(index=False)
    clean_upload_uri = export_file_to_s3(
        connection=s3_co, file_name=clean_upload_file_path, body=clean_df_as_parquet
    )
    print("Clean data uploaded at: ", clean_upload_uri)


if __name__ == "__main__":

    if input("Run this flow locally? [type yes]") == "yes":
        print("Running...\n", youbike_snapshots_ingestion())

    elif input("Deploy this flow [type yes]") == "yes":
        print("Deploying...")
        a = Deployment.build_from_flow(
            flow=youbike_snapshots_ingestion,
            output=f"flows/extract_youbike_data.yaml",
            name="youbike_snapshots_ingestion_stage",
            work_pool_name="ecs-stage",
            work_queue_name="default",
            schedules=[
                {
                    "schedule": {
                        "interval": 600,
                        "anchor_date": "2024-03-08T00:00:00+00:00",
                        "timezone": "Asia/Taipei",
                    },
                    "active": True,
                }
            ],
            path="/opt/prefect/flows",
            apply=True,
            load_existing=False,
        )
