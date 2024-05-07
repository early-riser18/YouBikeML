
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

import os
#DOCU https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#Getting_Started
#### CONFIG ####
sc = SparkContext()
sc.setLogLevel("DEBUG")
spark = SparkSession.builder \
    .appName("YouBike Test Spark") \
    .getOrCreate()

    # .config("spark.some.config.option", "some-value") \
if os.environ["APP_ENV"] == "stage":
    accessKeyId = os.environ["AWS_ACCESS_KEY_ID"]
    secretAccessKey = os.environ["AWS_SECRET_ACCESS_KEY"]
    sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint','s3.amazonaws.com')

else:
    accessKeyId = os.environ["MINIO_ACCESS_KEY_ID"]
    secretAccessKey = os.environ["MINIO_SECRET_ACCESS_KEY"]
    sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint',f'http://{os.environ["MINIO_HOST"]}:9000')


sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', accessKeyId)
sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', secretAccessKey)
sc._jsc.hadoopConfiguration().set('fs.s3a.path.style.access', 'true')
sc._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
conf = sc._jsc.hadoopConfiguration()



### RETRAINING CODE ###

# Import necessary Java classes
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem

bucket_fs = FileSystem.get(URI('s3a://local-youbike/clean_data/'), conf)

# print("conf: ",conf)
# print("sc.getConf().getAll(): ", [[k,v] for k, v in sc.getConf().getAll()])
# print("spark: ", spark)
status = bucket_fs.listStatus(Path('s3a://local-youbike/clean_data/'))

youbike_snapshot_uri = [obj.getPath().toString() for obj in status]
print("Files processed: ", len(status))
hist_snapshot_df = spark.read.option("InferSchema",True).option('header', True).format("parquet").load(youbike_snapshot_uri)

print(hist_snapshot_df.describe().show())

print("My LOG: Reached the END")