#!/bin/bash

SPARK_WORKLOAD=$1

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

if [ "$SPARK_WORKLOAD" == "master" ];
then
    start-master.sh -p 7077
elif [ "$SPARK_WORKLOAD" == "worker" ];
then
    start-worker.sh spark://spark-master:7077
elif [ "$SPARK_WORKLOAD" == "history" ];
then
    start-history-server.sh
elif [ "$SPARK_WORKLOAD" == "gateway" ];
then
    spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/spark/retrain_model.py /opt/spark/jars/hadoop-aws-3.3.4.jar /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar
fi
