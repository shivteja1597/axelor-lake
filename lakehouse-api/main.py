import os
import re
import time
import logging
from base64 import b64encode
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request as UrlRequest, urlopen
from typing import Any, BinaryIO

from pyarrow.lib import ArrowInvalid

import boto3
import psycopg
import pyarrow as pa
import pyarrow.csv as pacsv
from botocore.exceptions import ClientError, EndpointConnectionError
from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)
from ml.customer_risk import run_workflow as run_customer_risk_workflow
from ml.telecom_churn import run_prediction as run_telecom_churn_prediction
from ml.telecom_churn import run_training as run_telecom_churn_training

app = FastAPI(title="Lakehouse API")
LOG = logging.getLogger(__name__)

TABLE_NAME_RE = re.compile(r"^[A-Za-z0-9_]+$")
DEFAULT_QUERY_LIMIT = 100
MAX_QUERY_LIMIT = 1000
UPLOAD_BUFFER_SIZE = 4 * 1024 * 1024
EMPLOYEE_PROFILE_COLUMNS = {"employeeid", "name", "department", "age", "salary", "status"}
CUSTOMER_PROFILE_COLUMNS = {
    "account_id",
    "site_id",
    "customer_first_name",
    "customer_last_name",
    "address_line1",
    "address_line_2",
    "city",
    "state",
    "country",
}
TELECOM_CHURN_COLUMNS = {
    "customer_id",
    "name",
    "age",
    "gender",
    "tenure",
    "contract",
    "monthly_charges",
    "total_charges",
    "internet_service",
    "payment_method",
}
TELECOM_TRAINING_COLUMNS = {
    "customer_id",
    "name",
    "age",
    "gender",
    "tenure",
    "contract",
    "monthly_charges",
    "total_charges",
    "internet_service",
    "payment_method",
    "churn",
}

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "warehouse")
MINIO_RAW_BUCKET = os.getenv("MINIO_RAW_BUCKET", "lake-raw")
MINIO_CURATED_BUCKET = os.getenv("MINIO_CURATED_BUCKET", "lake-curated")
MINIO_ANALYTICS_BUCKET = os.getenv("MINIO_ANALYTICS_BUCKET", "lake-analytics")
MINIO_MODELS_BUCKET = os.getenv("MINIO_MODELS_BUCKET", "lake-models")
ICEBERG_NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "my_data")
CATALOG_URI = os.getenv("CATALOG_URI", os.getenv("ICEBERG_CATALOG_URI", "http://nessie:19120/iceberg/main"))
PGDUCKDB_DSN = os.getenv(
    "PGDUCKDB_DSN",
    "host=pgduckdb port=5433 dbname=analytics user=postgres password=duckdb",
)
JENKINS_URL = os.getenv("JENKINS_URL", "http://jenkins:8080")
JENKINS_JOB_URL = os.getenv("JENKINS_JOB_URL", "")
JENKINS_USER = os.getenv("JENKINS_USER", "")
JENKINS_API_TOKEN = os.getenv("JENKINS_API_TOKEN", "")
JENKINS_BUILD_TOKEN = os.getenv("JENKINS_BUILD_TOKEN", "")
JENKINS_WAIT_TIMEOUT_SECONDS = int(os.getenv("JENKINS_WAIT_TIMEOUT_SECONDS", "600"))
JENKINS_POLL_INTERVAL_SECONDS = float(os.getenv("JENKINS_POLL_INTERVAL_SECONDS", "2"))

_catalog: RestCatalog | None = None

def get_catalog() -> RestCatalog:
    global _catalog
    if _catalog is None:
        _catalog = load_catalog(
            "lakehouse",
            type="rest",
            uri=CATALOG_URI,
            **{
                "s3.endpoint": MINIO_ENDPOINT,
                "s3.access-key-id": MINIO_ACCESS_KEY,
                "s3.secret-access-key": MINIO_SECRET_KEY,
                "s3.region": MINIO_REGION,
            },
        )
    return _catalog

def minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name=MINIO_REGION,
    )

def ensure_bucket_exists() -> None:
    client = minio_client()
    try:
        client.head_bucket(Bucket=MINIO_BUCKET)
    except ClientError as err:
        error_code = err.response.get("Error", {}).get("Code", "")
        if error_code in {"404", "NoSuchBucket", "NotFound"}:
            create_args = {"Bucket": MINIO_BUCKET}
            if MINIO_REGION != "us-east-1":
                create_args["CreateBucketConfiguration"] = {"LocationConstraint": MINIO_REGION}
            client.create_bucket(**create_args)
        else:
            raise

def wait_for_minio() -> None:
    last_error: Exception | None = None
    for _ in range(30):
        try:
            ensure_bucket_exists()
            return
        except (EndpointConnectionError, ClientError) as err:
            last_error = err
            time.sleep(2)
    if last_error is not None:
        raise last_error

@app.on_event("startup")
def startup() -> None:
    wait_for_minio()
    try:
        catalog = get_catalog()
        ensure_namespace_exists(catalog)
    except Exception:
        pass  # Namespace will be created on first upload if startup fails

def validate_table_name(table_name: str) -> str:
    if not TABLE_NAME_RE.match(table_name):
        raise HTTPException(status_code=400, detail="Invalid table name")
    return table_name

def normalize_limit(limit: int | None) -> int | None:
    if limit is None or limit <= 0:
        return None
    return min(limit, MAX_QUERY_LIMIT)

def schema_from_arrow(table: pa.Table) -> Schema:
    fields = []
    for index, field in enumerate(table.schema):
        field_type = field.type
        iceberg_type = StringType()

        if pa.types.is_boolean(field_type):
            iceberg_type = BooleanType()
        elif pa.types.is_integer(field_type):
            iceberg_type = LongType()
        elif pa.types.is_floating(field_type):
            iceberg_type = DoubleType()
        elif pa.types.is_date(field_type):
            iceberg_type = DateType()
        elif pa.types.is_timestamp(field_type):
            iceberg_type = TimestampType()
        elif pa.types.is_binary(field_type):
            iceberg_type = BinaryType()

        fields.append(
            NestedField(
                field_id=index + 1,
                name=field.name,
                field_type=iceberg_type,
                required=False,
            )
        )

    return Schema(*fields)

def get_table_identifier(table_name: str) -> tuple[str, str]:
    return (ICEBERG_NAMESPACE, table_name)

def create_table(catalog: RestCatalog, identifier: tuple[str, str], schema: Schema):
    ensure_namespace_exists(catalog)
    return catalog.create_table(
        identifier=identifier,
        schema=schema,
        partition_spec=UNPARTITIONED_PARTITION_SPEC,
    )

def ensure_namespace_exists(catalog: RestCatalog) -> None:
    catalog.create_namespace_if_not_exists((ICEBERG_NAMESPACE,))

def delete_table_objects(table_name: str) -> None:
    prefix = f"{ICEBERG_NAMESPACE}/{table_name}/"
    delete_bucket_prefix(MINIO_BUCKET, prefix)

def delete_bucket_objects(bucket_name: str, object_keys: list[str]) -> int:
    if not object_keys:
        return 0

    client = minio_client()
    existing_keys: list[dict[str, str]] = []
    for object_key in object_keys:
        try:
            client.head_object(Bucket=bucket_name, Key=object_key)
            existing_keys.append({"Key": object_key})
        except ClientError as err:
            error_code = err.response.get("Error", {}).get("Code", "")
            if error_code in {"404", "NoSuchKey", "NoSuchBucket", "NotFound"}:
                continue
            raise

    deleted_count = 0
    for start in range(0, len(existing_keys), 1000):
        batch = existing_keys[start : start + 1000]
        if batch:
            client.delete_objects(Bucket=bucket_name, Delete={"Objects": batch})
            deleted_count += len(batch)
    return deleted_count

def delete_bucket_prefix(bucket_name: str, prefix: str) -> int:
    client = minio_client()
    paginator = client.get_paginator("list_objects_v2")
    keys = []

    try:
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            for content in page.get("Contents", []):
                keys.append({"Key": content["Key"]})
    except ClientError as err:
        error_code = err.response.get("Error", {}).get("Code", "")
        if error_code in {"404", "NoSuchBucket", "NotFound"}:
            return 0
        raise

    deleted_count = 0
    for start in range(0, len(keys), 1000):
        batch = keys[start : start + 1000]
        if batch:
            client.delete_objects(Bucket=bucket_name, Delete={"Objects": batch})
            deleted_count += len(batch)
    return deleted_count

def delete_bucket_prefixes(bucket_name: str, prefixes: list[str]) -> int:
    deleted_count = 0
    seen_prefixes: set[str] = set()
    for prefix in prefixes:
        if not prefix or prefix in seen_prefixes:
            continue
        seen_prefixes.add(prefix)
        deleted_count += delete_bucket_prefix(bucket_name, prefix)
    return deleted_count

def purge_bucket(bucket_name: str) -> int:
    return delete_bucket_prefix(bucket_name, "")

def purge_lakehouse_storage() -> dict[str, int]:
    return {
        "lake_raw": purge_bucket(MINIO_RAW_BUCKET),
        "lake_staging": purge_bucket(MINIO_BUCKET),
        "lake_curated": purge_bucket(MINIO_CURATED_BUCKET),
        "lake_analytics": purge_bucket(MINIO_ANALYTICS_BUCKET),
        "lake_models": purge_bucket(MINIO_MODELS_BUCKET),
    }

def recreate_table(catalog: RestCatalog, identifier: tuple[str, str], table_name: str, schema: Schema):
    ensure_namespace_exists(catalog)
    try:
        catalog.drop_table(identifier)
    except Exception:
        pass

    delete_table_objects(table_name)
    return create_table(catalog, identifier, schema)

def latest_metadata_path(table_name: str) -> str:
    validate_table_name(table_name)
    catalog = get_catalog()
    identifier = get_table_identifier(table_name)

    try:
        table = catalog.load_table(identifier)
    except NoSuchTableError as err:
        raise HTTPException(status_code=404, detail="Table not found") from err

    metadata_path = table.metadata_location
    if not metadata_path:
        raise HTTPException(status_code=404, detail="No metadata file found")

    return metadata_path

def get_latest_table_details() -> tuple[str, Any]:
    catalog = get_catalog()
    identifiers = catalog.list_tables((ICEBERG_NAMESPACE,))

    latest_identifier = None
    latest_table = None
    latest_updated_ms = -1

    for identifier in identifiers:
        table = catalog.load_table(identifier)
        updated_ms = int(getattr(getattr(table, "metadata", None), "last_updated_ms", 0) or 0)
        if updated_ms >= latest_updated_ms:
            latest_updated_ms = updated_ms
            latest_identifier = identifier
            latest_table = table

    if latest_identifier is None or latest_table is None:
        raise HTTPException(status_code=404, detail="No lakehouse tables were found.")

    table_name = latest_identifier[-1] if isinstance(latest_identifier, tuple) else str(latest_identifier)
    return str(table_name), latest_table

def detect_dataset_kind_from_table(table: Any) -> str:
    columns = [field.name for field in table.schema().fields]
    return detect_dataset_kind(set(columns))

def detect_dataset_kind(columns: set[str]) -> str:
    lowered = {column.lower() for column in columns}
    if EMPLOYEE_PROFILE_COLUMNS.issubset(lowered):
        return "employee_profile"
    if CUSTOMER_PROFILE_COLUMNS.issubset(lowered):
        return "customer_profile"
    if TELECOM_TRAINING_COLUMNS.issubset(lowered):
        return "telecom_training"
    if TELECOM_CHURN_COLUMNS.issubset(lowered) and "churn" not in lowered:
        return "telecom_prediction"
    return "unknown"

def get_dataset_artifacts(dataset_kind: str) -> dict[str, list[str]]:
    curated_objects: list[str] = []
    analytics_objects: list[str] = []
    model_objects: list[str] = []

    if dataset_kind == "employee_profile":
        curated_objects = [
            "dim_employee.parquet",
        ]
        analytics_objects = [
            "employee_role_summary.parquet",
            "employee_manager_summary.parquet",
            "employee_department_salary_summary.parquet",
            "employee_salary_band.parquet",
        ]
    elif dataset_kind == "customer_profile":
        curated_objects = [
            "customer_profile_features.parquet",
        ]
        analytics_objects = [
            "customer_predictions.parquet",
            "customer_segments.parquet",
        ]
        model_objects = [
            "customer-risk/logistic_regression.joblib",
        ]
    elif dataset_kind == "telecom_training":
        analytics_objects = [
            "telecom_customer_predictions.parquet",
        ]
        model_objects = [
            "telecom_churn/logistic_regression.joblib",
            "telecom_churn/model_metadata.json",
        ]
    elif dataset_kind == "telecom_prediction":
        analytics_objects = [
            "telecom_customer_predictions.parquet",
        ]

    return {
        "curated": curated_objects,
        "analytics": analytics_objects,
        "models": model_objects,
    }

def get_staging_location_from_metadata_path(metadata_path: str) -> tuple[str, str]:
    parsed = urlparse(metadata_path)
    bucket_name = parsed.netloc or MINIO_BUCKET
    object_key = parsed.path.lstrip("/")
    if not object_key:
        raise HTTPException(status_code=500, detail="Invalid metadata path: missing object key.")

    if "/metadata/" in object_key:
        table_prefix = object_key.split("/metadata/", 1)[0].rstrip("/") + "/"
    else:
        table_prefix = object_key.rsplit("/", 1)[0].rstrip("/") + "/"

    return bucket_name, table_prefix

def detect_csv_delimiter(csv_source: BinaryIO) -> str:
    csv_source.seek(0)
    sample = csv_source.read(4096)
    if isinstance(sample, bytes):
        sample_text = sample.decode("utf-8", errors="ignore")
    else:
        sample_text = str(sample)

    csv_source.seek(0)
    delimiter_scores = {",": sample_text.count(","), ";": sample_text.count(";"), "\t": sample_text.count("\t")}
    return max(delimiter_scores, key=delimiter_scores.get)

def read_csv_table(csv_source: BinaryIO) -> pa.Table:
    delimiter = detect_csv_delimiter(csv_source)
    csv_source.seek(0)
    with pa.input_stream(csv_source, buffer_size=UPLOAD_BUFFER_SIZE) as stream:
        return pacsv.read_csv(
            stream,
            read_options=pacsv.ReadOptions(use_threads=True, block_size=UPLOAD_BUFFER_SIZE),
            parse_options=pacsv.ParseOptions(delimiter=delimiter),
            convert_options=pacsv.ConvertOptions(strings_can_be_null=True),
        )

def build_query_sql(metadata_path: str, limit: int | None = None) -> str:
    limit_clause = f" LIMIT {limit}" if limit else ""
    escaped_path = metadata_path.replace("'", "''")
    return (
        "SELECT * FROM duckdb.query($$\n"
        f"    SELECT * FROM iceberg_scan('{escaped_path}'){limit_clause}\n"
        "$$)"
    )

def _jenkins_auth_header() -> dict[str, str]:
    if not JENKINS_USER or not JENKINS_API_TOKEN:
        return {}
    token = b64encode(f"{JENKINS_USER}:{JENKINS_API_TOKEN}".encode("utf-8")).decode("ascii")
    return {"Authorization": f"Basic {token}"}

def _jenkins_request(url: str, method: str = "GET", headers: dict[str, str] | None = None) -> tuple[int, str, dict[str, str]]:
    request = UrlRequest(url, method=method, headers=headers or {})
    try:
        with urlopen(request, timeout=30) as response:
            return response.status, response.read().decode("utf-8"), dict(response.headers.items())
    except HTTPError as err:
        body = err.read().decode("utf-8", errors="ignore")
        raise HTTPException(status_code=502, detail=f"Jenkins request failed: {err.code} {body}") from err
    except URLError as err:
        raise HTTPException(status_code=502, detail=f"Unable to reach Jenkins: {err.reason}") from err

def _jenkins_crumb_headers() -> dict[str, str]:
    headers = _jenkins_auth_header()
    status, body, _ = _jenkins_request(
        f"{JENKINS_URL.rstrip('/')}/crumbIssuer/api/json",
        headers={"Accept": "application/json", **headers},
    )
    if status != 200:
        raise HTTPException(status_code=502, detail="Unable to fetch Jenkins crumb.")
    import json

    payload = json.loads(body)
    crumb_field = payload.get("crumbRequestField")
    crumb_value = payload.get("crumb")
    if crumb_field and crumb_value:
        headers[crumb_field] = crumb_value
    return headers

def _extract_queue_location(response_headers: dict[str, str]) -> str:
    location = response_headers.get("Location") or response_headers.get("location")
    if not location:
        raise HTTPException(status_code=502, detail="Jenkins did not return a queue location.")
    return location.rstrip("/")

def _wait_for_queue_item(queue_url: str, deadline: float) -> tuple[int, str]:
    import json

    api_url = f"{queue_url}/api/json"
    while time.time() < deadline:
        status, body, _ = _jenkins_request(api_url, headers={"Accept": "application/json", **_jenkins_auth_header()})
        if status != 200:
            raise HTTPException(status_code=502, detail="Unable to inspect Jenkins queue item.")
        payload = json.loads(body)
        executable = payload.get("executable")
        if executable and payload.get("task", {}).get("name"):
            return int(executable["number"]), str(payload["task"]["name"])
        if payload.get("cancelled"):
            raise HTTPException(status_code=502, detail="Jenkins queue item was cancelled.")
        time.sleep(JENKINS_POLL_INTERVAL_SECONDS)
    raise HTTPException(status_code=504, detail="Timed out waiting for Jenkins job to start.")

def _wait_for_build_result(build_number: int, deadline: float) -> dict[str, Any]:
    import json

    build_api_url = f"{JENKINS_JOB_URL.rstrip('/')}/{build_number}/api/json"
    while time.time() < deadline:
        status, body, _ = _jenkins_request(build_api_url, headers={"Accept": "application/json", **_jenkins_auth_header()})
        if status != 200:
            raise HTTPException(status_code=502, detail="Unable to inspect Jenkins build result.")
        payload = json.loads(body)
        if not payload.get("building", False):
            return payload
        time.sleep(JENKINS_POLL_INTERVAL_SECONDS)
    raise HTTPException(status_code=504, detail="Timed out waiting for Jenkins build to finish.")

def enqueue_jenkins_pipeline() -> tuple[str, str]:
    if not JENKINS_JOB_URL:
        raise HTTPException(status_code=503, detail="JENKINS_JOB_URL is not configured.")

    trigger_url = f"{JENKINS_JOB_URL.rstrip('/')}/build"
    query_params: dict[str, str] = {}
    if JENKINS_BUILD_TOKEN:
        query_params["token"] = JENKINS_BUILD_TOKEN
    if query_params:
        trigger_url = f"{trigger_url}?{urlencode(query_params)}"

    headers = _jenkins_crumb_headers()
    status, _, response_headers = _jenkins_request(trigger_url, method="POST", headers=headers)
    if status not in {200, 201, 202}:
        raise HTTPException(status_code=502, detail="Jenkins did not accept the pipeline trigger.")

    queue_url = _extract_queue_location(response_headers)
    parsed_job_url = urlparse(JENKINS_JOB_URL.rstrip("/"))
    job_name = parsed_job_url.path.rstrip("/").split("/")[-1] or "unknown"
    return queue_url, job_name

def wait_for_jenkins_pipeline(queue_url: str, job_name: str) -> dict[str, Any]:
    deadline = time.time() + JENKINS_WAIT_TIMEOUT_SECONDS
    build_number, resolved_job_name = _wait_for_queue_item(queue_url, deadline)
    build_payload = _wait_for_build_result(build_number, deadline)
    result = build_payload.get("result")
    if result != "SUCCESS":
        raise HTTPException(
            status_code=502,
            detail=f"Jenkins pipeline failed with result: {result or 'UNKNOWN'}",
        )

    parsed_job_url = urlparse(JENKINS_JOB_URL.rstrip("/"))
    build_url = build_payload.get("url") or f"{parsed_job_url.geturl()}/{build_number}/"
    return {
        "job_name": resolved_job_name or job_name,
        "build_number": build_number,
        "result": result,
        "build_url": build_url,
    }

def trigger_jenkins_pipeline(wait_for_completion: bool = True) -> dict[str, Any]:
    queue_url, job_name = enqueue_jenkins_pipeline()
    if not wait_for_completion:
        return {
            "job_name": job_name,
            "result": "QUEUED",
            "queue_url": queue_url,
        }
    return wait_for_jenkins_pipeline(queue_url, job_name)

def run_jenkins_pipeline_in_background(job_name: str, queue_url: str) -> None:
    try:
        result = wait_for_jenkins_pipeline(queue_url, job_name)
        LOG.info("Background Jenkins pipeline completed for job %s: %s", job_name, result)
    except Exception:
        LOG.exception("Background Jenkins pipeline failed for job %s", job_name)


def run_customer_risk_after_pipeline(job_name: str, queue_url: str, table_name: str) -> None:
    try:
        pipeline_result = wait_for_jenkins_pipeline(queue_url, job_name)
        LOG.info(
            "Customer pipeline completed for table %s with result: %s",
            table_name,
            pipeline_result,
        )
        workflow_result = run_customer_risk_workflow(table_name)
        LOG.info(
            "Customer risk workflow completed for table %s: %s",
            table_name,
            workflow_result,
        )
    except Exception:
        LOG.exception("Customer risk background workflow failed for table %s", table_name)

@app.post("/upload")
async def upload(file: UploadFile = File(...), table_name: str = Form(...)) -> dict[str, Any]:
    table_name = validate_table_name(table_name)

    try:
        # Save exact raw file copy
        file.file.seek(0)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        raw_key = f"{table_name}/{table_name}_{timestamp}.csv"
        minio_client().put_object(
            Bucket=MINIO_RAW_BUCKET,
            Key=raw_key,
            Body=file.file,
            ContentType="text/csv"
        )
        file.file.seek(0)

        arrow_table = read_csv_table(file.file)
        catalog = get_catalog()
        ensure_namespace_exists(catalog)
        identifier = get_table_identifier(table_name)
        schema = schema_from_arrow(arrow_table)

        try:
            iceberg_table = recreate_table(catalog, identifier, table_name, schema)
        except Exception as err:
            if "NoSuchNamespaceException" not in str(err):
                raise
            ensure_namespace_exists(catalog)
            iceberg_table = recreate_table(catalog, identifier, table_name, schema)
        try:
            from pyiceberg.io.pyarrow import schema_to_pyarrow
            target_schema = schema_to_pyarrow(iceberg_table.schema())
            if arrow_table.schema.names == target_schema.names:
                arrow_table = arrow_table.cast(target_schema, safe=False)
        except Exception:
            pass

        iceberg_table.append(arrow_table)

        metadata_path = latest_metadata_path(table_name)
        return {
            "table_name": table_name,
            "metadata_path": metadata_path,
            "row_count": arrow_table.num_rows,
            "uploaded_at": datetime.now(timezone.utc).isoformat(),
        }
    except ArrowInvalid as err:
        raise HTTPException(status_code=400, detail=f"CSV parsing failed: {err}") from err
    except HTTPException:
        raise
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"Lakehouse upload failed: {err}") from err
    finally:
        await file.close()

@app.get("/query/{table_name}")
def query(
    table_name: str,
    limit: int = Query(DEFAULT_QUERY_LIMIT, ge=1, le=MAX_QUERY_LIMIT),
) -> dict[str, Any]:
    metadata_path = latest_metadata_path(table_name)
    sql = build_query_sql(metadata_path, normalize_limit(limit))
    with psycopg.connect(PGDUCKDB_DSN) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            columns = [desc.name for desc in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return {"table_name": table_name, "metadata_path": metadata_path, "rows": rows, "limit": limit}

@app.get("/tables")
def tables() -> dict[str, Any]:
    ensure_bucket_exists()
    prefix = f"{ICEBERG_NAMESPACE}/"
    paginator = minio_client().get_paginator("list_objects_v2")
    table_names = set()

    for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix=prefix):
        for content in page.get("Contents", []):
            key = content["Key"]
            parts = key.split("/")
            if len(parts) >= 3 and parts[0] == ICEBERG_NAMESPACE:
                table_names.add(parts[1])

    return {"tables": sorted(table_names)}

@app.get("/metadata/{table_name}")
def metadata(table_name: str) -> dict[str, Any]:
    return {"table_name": table_name, "metadata_path": latest_metadata_path(table_name)}

@app.post("/admin/purge-storage")
def purge_storage() -> dict[str, Any]:
    try:
        deleted_objects = purge_lakehouse_storage()
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"Unable to purge MinIO storage: {err}") from err
    return {
        "status": "ok",
        "deleted_objects": deleted_objects,
    }

@app.delete("/tables/{table_name}")
def delete_table(table_name: str) -> dict[str, Any]:
    table_name = validate_table_name(table_name)
    catalog = get_catalog()
    identifier = get_table_identifier(table_name)
    metadata_path: str | None = None
    staging_bucket = MINIO_BUCKET
    staging_prefix = f"{ICEBERG_NAMESPACE}/{table_name}/"
    dataset_kind = "unknown"

    try:
        table = catalog.load_table(identifier)
        metadata_path = table.metadata_location
        dataset_kind = detect_dataset_kind_from_table(table)
        if metadata_path:
            staging_bucket, staging_prefix = get_staging_location_from_metadata_path(metadata_path)
    except HTTPException as err:
        if err.status_code != 404:
            raise
    except NoSuchTableError:
        pass

    dropped_from_catalog = False
    try:
        catalog.drop_table(identifier)
        dropped_from_catalog = True
    except NoSuchTableError:
        pass
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"Unable to drop Iceberg table: {err}") from err

    try:
        staging_deleted = delete_bucket_prefix(staging_bucket, staging_prefix)
        raw_deleted = delete_bucket_prefix(MINIO_RAW_BUCKET, f"{table_name}/")
        artifacts = get_dataset_artifacts(dataset_kind)
        curated_deleted = delete_bucket_objects(
            MINIO_CURATED_BUCKET,
            artifacts["curated"],
        )
        analytics_deleted = delete_bucket_objects(
            MINIO_ANALYTICS_BUCKET,
            artifacts["analytics"],
        )
        models_deleted = delete_bucket_objects(
            MINIO_MODELS_BUCKET,
            artifacts["models"],
        )
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"Unable to delete MinIO objects: {err}") from err

    return {
        "table_name": table_name,
        "dataset_kind": dataset_kind,
        "metadata_path": metadata_path,
        "dropped_from_catalog": dropped_from_catalog,
        "deleted_objects": {
            "lake_staging": staging_deleted,
            "lake_raw": raw_deleted,
            "lake_curated": curated_deleted,
            "lake_analytics": analytics_deleted,
            "lake_models": models_deleted,
        },
    }

@app.post("/pipeline/run")
async def run_pipeline(
    background_tasks: BackgroundTasks,
    request: Request,
    table_name: str | None = Form(None),
    table_name_query: str | None = Query(None, alias="table_name"),
    wait: bool = Query(False),
    wait_form: bool | None = Form(None),
) -> dict[str, Any]:
    table_name = table_name or table_name_query
    if not table_name:
        content_type = request.headers.get("content-type", "")
        if "application/json" in content_type:
            payload = await request.json()
            if isinstance(payload, dict):
                table_name = payload.get("table_name")
                if "wait" in payload:
                    wait = bool(payload.get("wait"))
        elif "application/x-www-form-urlencoded" in content_type or "multipart/form-data" in content_type:
            form_data = await request.form()
            table_name = form_data.get("table_name")
            if form_data.get("wait") is not None:
                wait = str(form_data.get("wait")).strip().lower() in {"1", "true", "yes", "on"}
    if wait_form is not None:
        wait = wait_form
    if table_name:
        table_name = validate_table_name(table_name)
    if wait:
        pipeline = trigger_jenkins_pipeline(wait_for_completion=True)
    else:
        queue_url, job_name = enqueue_jenkins_pipeline()
        background_tasks.add_task(run_jenkins_pipeline_in_background, job_name, queue_url)
        pipeline = {
            "job_name": job_name,
            "result": "QUEUED",
            "queue_url": queue_url,
        }
    response = {"pipeline": pipeline}
    if table_name:
        response["table_name"] = table_name
    return response

@app.get("/datasets/latest")
def get_latest_dataset() -> dict[str, Any]:
    table_name, table = get_latest_table_details()
    columns = [field.name for field in table.schema().fields]
    metadata_path = table.metadata_location
    return {
        "table_name": table_name,
        "dataset_kind": detect_dataset_kind(set(columns)),
        "columns": columns,
        "metadata_path": metadata_path,
    }

@app.post("/ml/telecom-churn/run")
def run_telecom_churn() -> dict[str, Any]:
    try:
        result = run_telecom_churn_training()
    except HTTPException:
        raise
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"Unable to run telecom churn training: {err}") from err

    return result

@app.post("/ml/customer-risk/run")
def run_customer_risk(table_name: str = Query("customer_profile")) -> dict[str, Any]:
    try:
        result = run_customer_risk_workflow(table_name)
    except HTTPException:
        raise
    except Exception as err:
        raise HTTPException(
            status_code=500,
            detail=f"Unable to run customer risk workflow: {err}",
        ) from err

    return result


@app.post("/ml/customer-risk/trigger")
def trigger_customer_risk(
    background_tasks: BackgroundTasks,
    table_name: str = Query("customer_profile"),
    wait: bool = Query(False),
) -> dict[str, Any]:
    table_name = validate_table_name(table_name)

    if wait:
        pipeline = trigger_jenkins_pipeline(wait_for_completion=True)
        workflow = run_customer_risk_workflow(table_name)
        return {
            "table_name": table_name,
            "status": "completed",
            "pipeline": pipeline,
            "workflow": workflow,
        }

    queue_url, job_name = enqueue_jenkins_pipeline()
    background_tasks.add_task(run_customer_risk_after_pipeline, job_name, queue_url, table_name)
    return {
        "table_name": table_name,
        "status": "queued",
        "pipeline": {
            "job_name": job_name,
            "result": "QUEUED",
            "queue_url": queue_url,
        },
        "message": "Customer risk workflow queued in background.",
    }

@app.post("/ml/telecom-churn/train")
def train_telecom_churn(table_name: str = Query("telecom_customer_training")) -> dict[str, Any]:
    try:
        result = run_telecom_churn_training(table_name)
    except HTTPException:
        raise
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"Unable to train telecom churn model: {err}") from err
    return result

@app.post("/ml/telecom-churn/predict")
def predict_telecom_churn(table_name: str = Query("telecom_customer_data")) -> dict[str, Any]:
    try:
        result = run_telecom_churn_prediction(table_name)
    except HTTPException:
        raise
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"Unable to score telecom prediction data: {err}") from err
    return result
