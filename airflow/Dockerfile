FROM apache/airflow:2.10.2-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential libpq-dev curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    "duckdb>=1.1.0" \
    "dbt-core>=1.8.0,<1.10.0" \
    "dbt-duckdb>=1.8.0,<1.10.0" \
    "boto3>=1.35.0" \
    "pydantic-settings>=2.4.0" \
    "pyarrow>=16.0.0"
