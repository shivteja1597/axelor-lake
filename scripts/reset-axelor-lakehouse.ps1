param(
  [string]$DbHost = "localhost",
  [int]$DbPort = 5432,
  [string]$DbName = "axelor4",
  [string]$DbUser = "postgres",
  [string]$DbPassword = "root",
  [string]$AdminDb = "postgres",
  [string]$LakehouseApiBaseUrl = "http://localhost:8000"
)

$ErrorActionPreference = "Stop"

Write-Host "Dropping and recreating database '$DbName' on $DbHost:$DbPort..."

@"
import psycopg2

db_host = r"$DbHost"
db_port = $DbPort
db_name = r"$DbName"
db_user = r"$DbUser"
db_password = r"$DbPassword"
admin_db = r"$AdminDb"

conn = psycopg2.connect(
    host=db_host,
    port=db_port,
    dbname=admin_db,
    user=db_user,
    password=db_password,
)
conn.autocommit = True
cur = conn.cursor()

cur.execute(
    "SELECT pg_terminate_backend(pid) "
    "FROM pg_stat_activity "
    "WHERE datname = %s AND pid <> pg_backend_pid()",
    (db_name,),
)
cur.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
cur.execute(f'CREATE DATABASE "{db_name}"')

cur.close()
conn.close()
print("database reset complete")
"@ | python -

Write-Host "Purging MinIO lakehouse buckets via $LakehouseApiBaseUrl/admin/purge-storage ..."
$purgeResponse = Invoke-RestMethod -Method Post -Uri "$LakehouseApiBaseUrl/admin/purge-storage"
$purgeResponse | ConvertTo-Json -Depth 5

Write-Host "Reset complete."
