import boto3
import os
import pandas as pd


class ConnectionToS3:
    """Factory method object to create an active boto3 S3 resource. Ensure the correct env variables are set before calling this object.

    Available class method:
        from_env(): create connection from environment variables

    """

    def __init__(
        self,
        bucket_name: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        endpoint_url: str = None,
        region_name: str = "ap-northeast-1",
    ):
        self._resource = boto3.resource(
            "s3",
            endpoint_url=endpoint_url,
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
            return cls(
                "local-youbike",
                os.environ["MINIO_ACCESS_KEY_ID"],
                os.environ["MINIO_SECRET_ACCESS_KEY"],
                'http://localhost:9000'
            )
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
    """Upload a csv or text-like file to an s3 bucket at the specified path

    Return: URI of the uploaded object"""
    obj = connection.resource.Object(connection.bucket_name, file_name)
    obj.put(Body=body)
    return f"s3://{connection.bucket_name}/{file_name}"


if __name__ == "__main__":
    s3_co = ConnectionToS3.from_env()

    df_as_csv = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).to_csv()
    export_csv_to_s3(
        connection=s3_co, file_name="unit_test/unit_test.csv", body=df_as_csv
    )
