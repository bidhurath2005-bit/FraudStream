# FraudStream Spark Structured Streaming image.
# Spark runs in LOCAL mode (one JVM, N worker threads) inside this container; the host
# needs no Java. Project source and model artifacts are mounted READ-ONLY at runtime.
FROM python:3.12-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive PIP_NO_CACHE_DIR=1 PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps tini \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

COPY infrastructure/spark/requirements-spark.txt /tmp/requirements-spark.txt
RUN pip install -r /tmp/requirements-spark.txt

# Pre-fetch the Kafka connector (and its dependencies) into an Ivy cache at build time,
# so the job starts without network access.
ARG SPARK_KAFKA_PACKAGE=org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.5
ENV SPARK_KAFKA_PACKAGE=${SPARK_KAFKA_PACKAGE} SPARK_IVY_DIR=/opt/ivy
RUN python -c "from pyspark.sql import SparkSession; s=SparkSession.builder.master('local[1]').config('spark.jars.packages','${SPARK_KAFKA_PACKAGE}').config('spark.jars.ivy','/opt/ivy').getOrCreate(); print('spark', s.version); s.stop()"

RUN useradd -m -u 10001 spark && mkdir -p /checkpoints && chown -R spark:spark /checkpoints /opt/ivy
USER spark
WORKDIR /app
ENV PYTHONPATH=/app:/app/backend PYSPARK_PYTHON=python3 PYSPARK_DRIVER_PYTHON=python3
EXPOSE 4040
ENTRYPOINT ["tini", "--"]
CMD ["python", "-m", "streaming.spark.fraud_stream_job"]
