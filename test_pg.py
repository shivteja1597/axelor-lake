import psycopg

DSN = "host=pgduckdb port=5432 dbname=analytics user=postgres password=duckdb"
try:
    with psycopg.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM duckdb.query('SELECT * FROM read_parquet(''s3://lake-analytics/employee_role_summary.parquet'')')")
            for row in cur.fetchall():
                print(row)
except Exception as e:
    print(e)
