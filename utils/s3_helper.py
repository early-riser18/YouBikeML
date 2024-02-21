import boto3
import os
import pandas as pd


class ConnectionToS3:
    def __init__(
        self,
        bucket_name: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        region_name: str = "ap-northeast-1",
    ):
        self._resource = boto3.resource(
            "s3",
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        self._bucket_name = bucket_name

    @classmethod
    def from_env(cls):
        app_env = os.getenv("APP_ENV", "local")
        print("Loading from env: ", app_env)
        if app_env == "local":
            raise Exception("local env not supported")
        elif app_env == "stage":
            return cls(
                "stage-youbike",
                os.environ["AWS_ACCESS_KEY_ID"],
                os.environ["AWS_SECRET_ACCESS_KEY"],
            )
        else:
            raise Exception(f"The argument env={app_env} is not valid.")

    @property
    def resource(self):
        return self._resource

    @property
    def bucket_name(self):
        return self._bucket_name


def export_csv_to_s3(connection: ConnectionToS3, file_name: str, body=None) -> str:
    obj = connection.resource.Object(connection.bucket_name, file_name)
    obj.put(Body=body)
    return f"s3://{connection.bucket_name}/{file_name}"


if __name__ == "__main__":
    s3_co = ConnectionToS3.from_env()

    df_as_csv = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).to_csv()
    export_csv_to_s3(
        connection=s3_co, file_name="unit_test/unit_test.csv", body=df_as_csv
    )
