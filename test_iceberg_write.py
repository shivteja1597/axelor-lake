import duckdb

conn = duckdb.connect(':memory:')
conn.execute("INSTALL httpfs; LOAD httpfs;")
conn.execute("INSTALL iceberg; LOAD iceberg;")
conn.execute("SET s3_endpoint='minio:9000';")
conn.execute("SET s3_access_key_id='admin';")
conn.execute("SET s3_secret_access_key='password123';")
conn.execute("SET s3_region='us-east-1';")
conn.execute("SET s3_use_ssl=false;")
conn.execute("SET s3_url_style='path';")

conn.execute("""
    ATTACH 'warehouse' AS iceberg_lake 
    (TYPE iceberg, ENDPOINT 'http://nessie:19120/iceberg', AUTHORIZATION_TYPE 'none')
""")

# Create schema if not exists
try:
    conn.execute("CREATE SCHEMA IF NOT EXISTS iceberg_lake.lake_curated;")
    print("Schema created/verified")
except Exception as e:
    print("Schema creation failed:", e)

# Try writing
try:
    conn.execute("CREATE TABLE iceberg_lake.lake_curated.test_write (id INT);")
    print("Table created successfully")
    conn.execute("INSERT INTO iceberg_lake.lake_curated.test_write VALUES (1), (2);")
    print("Inserted successfully")
except Exception as e:
    print("Write failed:", e)

# Query back
try:
    print("Querying table:")
    print(conn.execute("SELECT * FROM iceberg_lake.lake_curated.test_write").fetchall())
except Exception as e:
    print("Query failed:", e)

# Show all tables
try:
    print("Show all tables:")
    print(conn.execute("SHOW ALL TABLES").fetchall())
except Exception as e:
    print("Show all tables failed:", e)
