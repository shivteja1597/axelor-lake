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
    con.execute("BEGIN TRANSACTION;")
    con.execute("COMMIT; DROP TABLE IF EXISTS iceberg_lake.lake_curated.my_table5;")
    con.execute("CREATE TABLE iceberg_lake.lake_curated.my_table5 AS SELECT 1 AS a; BEGIN TRANSACTION;")
    con.execute("COMMIT;")
    res = con.execute("SELECT * FROM iceberg_lake.lake_curated.my_table5;").fetchall()
    print("DATA:", res)
except Exception as e:
    print("ERROR:", e)
