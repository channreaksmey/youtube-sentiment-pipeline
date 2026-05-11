FROM ubuntu:22.04

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 \
    SPARK_VERSION=4.1.2 \
    SPARK_HOME=/opt/spark \
    PATH=$PATH:/opt/spark/bin

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    wget \
    bash \
    && rm -rf /var/lib/apt/lists/*

RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mkdir -p /opt/spark/logs

WORKDIR /opt/spark

EXPOSE 8080 7077 4040

CMD ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"]
