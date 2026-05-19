docker exec -i lakehouse-api bash << 'EOF'
python -c "
import psycopg
print('Connecting...')
conn = psycopg.connect('host=pgduckdb port=5432 dbname=analytics user=postgres password=duckdb')
cur = conn.cursor()
cur.execute(\"SELECT * FROM duckdb.query('SELECT * FROM read_parquet(''s3://lake-analytics/employee_role_summary.parquet'')')\")
print(cur.fetchall())
"
EOF
