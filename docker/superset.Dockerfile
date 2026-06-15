FROM apache/superset:4.1.2
RUN pip install psycopg2-binary trino
