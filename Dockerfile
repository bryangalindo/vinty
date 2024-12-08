FROM quay.io/astronomer/astro-runtime:12.5.0

WORKDIR /usr/local/airflow

COPY dbt-requirements.txt ./

RUN python3 -m venv dbt_env && \
    source dbt_env/bin/activate && \
    pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r dbt-requirements.txt && \
    deactivate

USER root

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN apt-get update --allow-insecure-repositories && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        openjdk-17-jdk ant && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER astro
