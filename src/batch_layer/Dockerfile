#FROM apache/spark:3.5.1-scala2.12-java17-python3-r-ubuntu
#
#USER root
#
#WORKDIR /app
#
#RUN /opt/spark/bin/spark-sql \
#    --master "local[*]" \
#    --packages org.apache.hadoop:hadoop-aws:3.3.4,\
#com.amazonaws:aws-java-sdk-bundle:1.12.262,\
#org.wildfly.openssl:wildfly-openssl:1.0.7.Final \
#    -e "SELECT 1;"
#
#COPY batch_requirements.txt .
#RUN pip install --no-cache-dir -r batch_requirements.txt
#
#ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/
#ENV PYTHONPATH=/app/src
#ENV APP_ENV=prod
#
#COPY src/common ./src/common
#COPY src/batch_layer ./src/batch_layer
#
#USER root

FROM python:3.10-slim-bookworm

RUN apt-get update && \
    apt install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    procps && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY batch_requirements.txt .
RUN pip install --no-cache-dir -r batch_requirements.txt

# Cache jars
RUN python3 -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.master('local[*]').config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.wildfly.openssl:wildfly-openssl:1.0.7.Final').getOrCreate(); spark.stop()"

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/
ENV PYTHONPATH=/app/src
ENV APP_ENV=prod

COPY src/common ./src/common
COPY src/batch_layer ./src/batch_layer