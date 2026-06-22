import duckdb

conn = duckdb.connect(':memory:')
print("Connected to :memory:")

# Attach Iceberg Catalog
try:
    conn.execute("""
        ATTACH 'warehouse' AS iceberg_lake 
        (TYPE iceberg, ENDPOINT 'http://nessie:19120/iceberg', AUTHORIZATION_TYPE 'none')
    """)
    print("Attached Iceberg Catalog successfully")
except Exception as e:
    print(f"Failed to attach Iceberg: {e}")

# Create View in memory.main
try:
    conn.execute("CREATE VIEW memory.main.test_view AS SELECT 123 AS value;")
    print("Created view in memory.main")
except Exception as e:
    print(f"Failed to create view: {e}")

# Read from view
try:
    res = conn.execute("SELECT * FROM memory.main.test_view").fetchall()
    print("Result from memory.main.test_view:", res)
except Exception as e:
    print(f"Failed to query view: {e}")

# Show tables
try:
    print("Tables list:")
    for row in conn.execute("SHOW ALL TABLES;").fetchall():
        print(row)
except Exception as e:
    print(f"Failed to show tables: {e}")
