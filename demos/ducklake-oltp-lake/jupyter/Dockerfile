FROM jupyter/scipy-notebook:latest

# Install DuckDB and PostgreSQL adapter for DuckLake catalog queries
RUN pip install --no-cache-dir \
    duckdb \
    duckdb-engine \
    psycopg2-binary \
    s3fs
