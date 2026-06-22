import duckdb

con = duckdb.connect()
try:
    con.execute("ATTACH 'warehouse' AS iceberg_lake (TYPE iceberg, ENDPOINT 'http://nessie:19120/iceberg', AUTHORIZATION_TYPE 'none');")
    res = con.execute("SHOW TABLES FROM iceberg_lake.lake_curated;").fetchall()
    print("TABLES IN lake_curated:", res)
except Exception as e:
    print("ERROR:", e)
