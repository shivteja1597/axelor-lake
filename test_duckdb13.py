import duckdb

con = duckdb.connect()
con.execute("SET s3_endpoint='minio:9000';")
con.execute("SET s3_access_key_id='admin';")
con.execute("SET s3_secret_access_key='password123';")
con.execute("SET s3_region='us-east-1';")
con.execute("SET s3_use_ssl=false;")
con.execute("SET s3_url_style='path';")
try:
    con.execute("ATTACH 'warehouse' AS iceberg_lake (TYPE iceberg, ENDPOINT 'http://nessie:19120/iceberg', AUTHORIZATION_TYPE 'none');")
    con.execute("CREATE SCHEMA IF NOT EXISTS iceberg_lake.lake_curated;")
    con.execute("CREATE TABLE IF NOT EXISTS iceberg_lake.lake_curated.my_table4 AS SELECT 1 AS a;")
    con.execute("CREATE OR REPLACE TABLE iceberg_lake.lake_curated.my_table4 AS SELECT 2 AS a;")
    res = con.execute("SELECT * FROM iceberg_lake.lake_curated.my_table4;").fetchall()
    print("DATA:", res)
except Exception as e:
    print("ERROR:", e)
