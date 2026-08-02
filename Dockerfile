FROM ghcr.io/astral-sh/uv:0.11.7 AS uv

FROM apache/airflow:3.3.0

COPY --from=uv /uv /usr/local/bin/uv

# Install only third-party runtime dependencies. Airflow itself is supplied by
# the base image, while project code is made importable through PYTHONPATH.
COPY --chown=airflow:root pyproject.toml uv.lock /opt/airflow/project/
WORKDIR /opt/airflow/project
RUN uv export \
      --frozen \
      --no-dev \
      --no-emit-project \
      --output-file /tmp/requirements.txt \
    && pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      -r /tmp/requirements.txt

COPY --chown=airflow:root src /opt/airflow/src
COPY --chown=airflow:root dags /opt/airflow/dags

ENV PYTHONPATH=/opt/airflow/src
WORKDIR /opt/airflow
