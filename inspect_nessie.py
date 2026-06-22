from pyiceberg.catalog import load_catalog

cat = load_catalog(
    "lh",
    type="rest",
    uri="http://nessie:19120/iceberg",
    **{
        "s3.endpoint": "http://minio:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password123",
        "s3.region": "us-east-1",
    },
)
print("Namespaces:", cat.list_namespaces())

for ns in cat.list_namespaces():
    try:
        print(f"Tables under {ns}:", cat.list_tables(ns))
    except Exception as e:
        print(f"Error listing tables under {ns}: {e}")
