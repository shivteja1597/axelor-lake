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

try:
    print("Listing all tables:")
    res = conn.execute("SHOW ALL TABLES;").fetchall()
    for row in res:
        print(row)
except Exception as e:
    print(f"Error listing tables: {e}")
