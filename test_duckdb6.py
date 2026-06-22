import duckdb

con = duckdb.connect()
try:
    con.execute("ATTACH 'warehouse' AS iceberg_lake (TYPE iceberg, ENDPOINT 'http://nessie:19120/iceberg', AUTHORIZATION_TYPE 'none');")
    con.execute("CREATE SCHEMA IF NOT EXISTS iceberg_lake.lake_curated;")
    print("SCHEMA created")
    con.execute("CREATE TABLE iceberg_lake.lake_curated.my_table AS SELECT 1 AS a;")
    print("TABLE created")
    res = con.execute("SELECT * FROM iceberg_lake.lake_curated.my_table;").fetchall()
    print("DATA:", res)
except Exception as e:
    print("ERROR:", e)
