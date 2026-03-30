FROM apache/airflow:2.7.1-python3.11

USER root
## Install container
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-11-jdk && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

## switch to airflow user
USER airflow

RUN pip install  --no-cache-dir\
    apache-airflow-providers-apache-spark==4.1.5 \
    pyspark==3.5.1