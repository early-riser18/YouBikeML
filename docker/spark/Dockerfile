FROM python:3.11-bullseye as spark-base
ARG SPARK_VERSION=3.3.4


### WARNING: Dockerfile should be run with repo's root dir as context. 

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    sudo \
    curl \
    vim \
    unzip \
    rsync \
    openjdk-11-jdk \
    build-essential \
    software-properties-common \ 
    ssh && \
apt-get clean && \
rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}
WORKDIR $SPARK_HOME

## GET SPARK AND HADOOP
RUN curl https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o spark-${SPARK_VERSION}-bin-hadoop3.tgz \
&& tar xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
&& rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz

## GET JAR DEPENDENCIES

RUN curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${SPARK_VERSION}/hadoop-aws-${SPARK_VERSION}.jar \
    -o $SPARK_HOME/jars/hadoop-aws-${SPARK_VERSION}.jar \
&& curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    -o $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.262.jar



COPY ./docker/spark/requirements.txt .
RUN pip3 install -r requirements.txt

ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV SPARK_HOME="/opt/spark"
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3
# COPY conf/spark-defaults.conf "$SPARK_HOME/conf"
RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

COPY ./docker/spark/entrypoint.sh .
RUN ["chmod", "a+x", "./entrypoint.sh"]
COPY ./retrain-script.py .



### TO REFACTOR
FROM spark-base as build1
WORKDIR $SPARK_HOME
COPY --from=spark-base $SPARK_HOME/retrain-script.py .
ENV MINIO_ACCESS_KEY_ID=minio-local
ENV MINIO_SECRET_ACCESS_KEY=minio-local
ENV MINIO_HOST=minio-server
ENV APP_ENV=local
ENTRYPOINT [ "./entrypoint.sh" ]