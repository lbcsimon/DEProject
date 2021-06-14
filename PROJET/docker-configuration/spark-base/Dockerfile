FROM ubuntu

ARG spark_version=3.0.0
ARG hadoop_version=2.7

RUN apt-get -y update 
RUN apt-get -y install python3
RUN apt-get install -y openjdk-8-jre
RUN apt-get install -y wget
RUN wget https://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop${hadoop_version}.tgz
RUN tar -xf spark-${spark_version}-bin-hadoop${hadoop_version}.tgz
RUN mv spark-${spark_version}-bin-hadoop${hadoop_version} spark
RUN mkdir spark/logs
RUN rm spark*.tgz

ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3
