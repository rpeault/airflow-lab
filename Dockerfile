# Lab image: official Airflow + extra providers not in apache/airflow:*.
ARG AIRFLOW_VERSION=3.3.1
FROM apache/airflow:${AIRFLOW_VERSION}

ARG AIRFLOW_VERSION
COPY requirements-docker.txt /tmp/requirements-docker.txt

USER airflow
RUN PY=$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")') \
 && pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      -r /tmp/requirements-docker.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY}.txt"
