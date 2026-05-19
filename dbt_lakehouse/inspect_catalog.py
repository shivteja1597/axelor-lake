from pyiceberg.catalog import load_catalog
cat = load_catalog(
    "lh",
    type="rest",
    uri="http://nessie:19120/iceberg/main",
    **{
        "s3.endpoint": "http://minio:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password123",
        "s3.region": "us-east-1",
    },
)
tables = cat.list_tables("my_data")
print("Tables:", tables)
for t in tables:
    tbl = cat.load_table(t)
    print(f"Table: {t}, Metadata: {tbl.metadata_location}")
    print(f"Schema: {tbl.schema()}")
