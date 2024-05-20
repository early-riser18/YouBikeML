### WARNING: Dockerfile expect the build context to be the repo's root directory
FROM python:3.10-bullseye as spark-base
ARG SPARK_VERSION=3.3.4

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

# Set up Spark environments and entry point
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV SPARK_HOME="/opt/spark"
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_PORT 7077
ENV SPARK_MASTER_HOST spark-master
RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*
    
## Set up python dependencies 
COPY ./docker/spark/requirements.txt .
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt 
ENV PYSPARK_PYTHON python3
ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH:$SPARK_HOME/app

FROM spark-base as build1
WORKDIR $SPARK_HOME
COPY ./docker/spark/entrypoint.sh .


RUN ["chmod", "a+x", "./entrypoint.sh"]
ENTRYPOINT [ "./entrypoint.sh" ]