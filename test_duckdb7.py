import duckdb

con = duckdb.connect()
try:
    con.execute("ATTACH 'warehouse' AS iceberg_lake (TYPE iceberg, ENDPOINT 'http://nessie:19120/iceberg', AUTHORIZATION_TYPE 'none');")
    res = con.execute("SHOW TABLES FROM iceberg_lake.lake_curated;").fetchall()
    print("TABLES:", res)
    res2 = con.execute("SELECT * FROM information_schema.tables WHERE table_catalog='iceberg_lake';").fetchall()
    print("ALL TABLES:", res2)
except Exception as e:
    print("ERROR:", e)
