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
    print("Switching catalog to iceberg_lake...")
    conn.execute("USE iceberg_lake;")
    print("Listing tables in iceberg_lake:")
    res = conn.execute("SHOW TABLES;").fetchall()
    for row in res:
        print(row)
    
    print("\nQuerying information_schema.tables:")
    res2 = conn.execute("SELECT database_name, schema_name, table_name FROM information_schema.tables;").fetchall()
    for row in res2:
        print(row)
except Exception as e:
    print(f"Error: {e}")
