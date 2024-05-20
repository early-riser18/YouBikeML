from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os
from pandas import DataFrame

# Fix due to pandas 2.0 and pyspark 3.3 incompatibility
DataFrame.iteritems = DataFrame.items

class SparkApp:
    __unique_instance = None

    def __init__(self, log_level: str = "WARN") -> None:
        if self.__unique_instance is not None:
            raise Exception(
                f"You are trying to initialize existing object {self}. Use get_instance() instead."
            )

        self.spark_conf = self.__get_spark_config()
        self.spark_context = SparkContext(conf=self.__get_spark_config())
        self.spark_context.setLogLevel(log_level)
        self.spark_hadoop_conf = self.spark_context._jsc.hadoopConfiguration()
        self.__spark_session = SparkSession.builder.getOrCreate()

        SparkApp.__unique_instance = self

    @property
    def spark_session(self) -> SparkSession:
        """Allows for type hinting in development"""
        return self.__spark_session

    @classmethod
    def get_instance(cls, log_level: str = "WARN"):
        if cls.__unique_instance is None:
            cls.__unique_instance = cls(log_level)
        return cls.__unique_instance

    def __get_spark_config(self) -> SparkConf:
        _spark_conf = SparkConf()

        if os.environ["APP_ENV"] == "stage":
            accessKeyId = os.environ["AWS_ACCESS_KEY_ID"]
            secretAccessKey = os.environ["AWS_SECRET_ACCESS_KEY"]
            _spark_conf.set("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        else:
            accessKeyId = os.environ["MINIO_ACCESS_KEY_ID"]
            secretAccessKey = os.environ["MINIO_SECRET_ACCESS_KEY"]
            _spark_conf.set(
                "spark.hadoop.fs.s3a.endpoint",
                f'http://{os.environ["MINIO_HOST"]}:9000',
            )

        _spark_conf.set(
            "spark.sql.legacy.parquet.nanosAsLong", "true"
        )  # Fixes a compatibility issue with parquet
        _spark_conf.set("spark.hadoop.fs.s3a.access.key", accessKeyId)
        _spark_conf.set("spark.hadoop.fs.s3a.secret.key", secretAccessKey)
        _spark_conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
        _spark_conf.set(
            "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )

        return _spark_conf

   