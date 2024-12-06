FROM quay.io/astronomer/astro-runtime:12.5.0

USER $AIRFLOW_UID

WORKDIR /usr/local/airflow
COPY dbt-requirements.txt ./

RUN python3 -m venv dbt_env && source dbt_env/bin/activate && \
    pip install --upgrade pip && pip install --no-cache-dir -r dbt-requirements.txt && deactivate

USER root

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64
RUN export JAVA_HOME

RUN apt update --allow-insecure-repositories && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

USER astro
