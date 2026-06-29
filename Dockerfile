FROM apache/airflow:3.2.2-python3.12

COPY pyproject.toml uv.lock /opt/airflow/
RUN uv export --no-dev --no-hashes -o /tmp/reqs.txt && \
    uv pip install --no-cache -r /tmp/reqs.txt