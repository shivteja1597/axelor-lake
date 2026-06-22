import psycopg

DSN = "host=pgduckdb port=5432 dbname=analytics user=postgres password=duckdb"
try:
    with psycopg.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT duckdb.raw_query('INSTALL httpfs')")
            print("INSTALL OK")
            cur.execute("SELECT duckdb.raw_query('LOAD httpfs')")
            print("LOAD OK")
            cur.execute("SELECT * FROM duckdb.query('SELECT 1 AS a')")
            print(cur.fetchall())
except Exception as e:
    print("ERROR:", e)
