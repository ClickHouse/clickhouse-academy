FROM apache/superset:3.1.0

USER root

# Install database drivers:
# - snowflake-sqlalchemy: Snowflake connection (Act 1)
# - clickhouse-connect: ClickHouse connection (Act 2)
RUN pip install --no-cache-dir \
    snowflake-sqlalchemy==1.6.1 \
    snowflake-connector-python==3.7.0 \
    clickhouse-connect==0.7.0

USER superset
