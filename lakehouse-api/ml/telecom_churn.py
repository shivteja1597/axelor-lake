import io
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
import joblib
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from pyiceberg.catalog import load_catalog
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

FEATURE_COLUMNS = {
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
TRAINING_COLUMNS = {
    *FEATURE_COLUMNS,
    "churn",
}
PREDICTIONS_OBJECT_KEY = "telecom_customer_predictions.parquet"
MODEL_OBJECT_KEY = "telecom_churn/logistic_regression.joblib"
MODEL_METADATA_OBJECT_KEY = "telecom_churn/model_metadata.json"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")
MINIO_ANALYTICS_BUCKET = os.getenv("MINIO_ANALYTICS_BUCKET", "lake-analytics")
MINIO_MODELS_BUCKET = os.getenv("MINIO_MODELS_BUCKET", "lake-models")
CATALOG_URI = os.getenv("CATALOG_URI", os.getenv("ICEBERG_CATALOG_URI", "http://nessie:19120/iceberg/main"))
ICEBERG_NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "my_data")


def minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name=MINIO_REGION,
    )


def ensure_bucket_exists(bucket_name: str) -> None:
    client = minio_client()
    try:
        client.head_bucket(Bucket=bucket_name)
    except ClientError as err:
        error_code = err.response.get("Error", {}).get("Code", "")
        if error_code in {"404", "NoSuchBucket", "NotFound"}:
            create_args = {"Bucket": bucket_name}
            if MINIO_REGION != "us-east-1":
                create_args["CreateBucketConfiguration"] = {"LocationConstraint": MINIO_REGION}
            client.create_bucket(**create_args)
        else:
            raise


def get_catalog():
    return load_catalog(
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


def get_table_name(identifier) -> str:
    if isinstance(identifier, tuple):
        return str(identifier[-1])
    return str(identifier)


def get_table_last_updated_ms(table) -> int:
    metadata = getattr(table, "metadata", None)
    return int(getattr(metadata, "last_updated_ms", 0) or 0)


def get_table_columns(table) -> set[str]:
    schema = table.schema()
    return {field.name.lower() for field in schema.fields}


def get_latest_table(catalog):
    identifiers = catalog.list_tables((ICEBERG_NAMESPACE,))
    latest_identifier = None
    latest_table = None
    latest_updated_ms = -1

    for identifier in identifiers:
        table = catalog.load_table(identifier)
        updated_ms = get_table_last_updated_ms(table)
        if updated_ms >= latest_updated_ms:
            latest_updated_ms = updated_ms
            latest_identifier = identifier
            latest_table = table

    return latest_identifier, latest_table


def load_table_dataframe(table_name: str, required_columns: set[str], forbidden_columns: set[str] | None = None) -> pd.DataFrame:
    catalog = get_catalog()
    identifier = (ICEBERG_NAMESPACE, table_name)
    table = catalog.load_table(identifier)
    columns = get_table_columns(table)
    if not required_columns.issubset(columns):
        raise LookupError(
            f"Table {table_name} does not match the required telecom schema. "
            f"Required columns: {sorted(required_columns)}"
        )
    if forbidden_columns and columns.intersection({column.lower() for column in forbidden_columns}):
        raise LookupError(
            f"Table {table_name} contains forbidden columns: {sorted(forbidden_columns)}"
        )
    dataframe = table.scan().to_arrow().to_pandas()
    dataframe.columns = [str(column).strip().lower() for column in dataframe.columns]
    return dataframe


def normalize_churn_value(value):
    if pd.isna(value):
        return None
    text = str(value).strip().lower()
    if text in {"yes", "y", "1", "true", "leave", "left"}:
        return 1
    if text in {"no", "n", "0", "false", "stay", "stayed"}:
        return 0
    return None


def build_predictions(dataframe: pd.DataFrame) -> tuple[pd.DataFrame, Pipeline]:
    working = dataframe.copy()
    working["customer_id"] = working["customer_id"].astype(str).str.strip()
    working["name"] = working["name"].astype(str).str.strip()
    working["age"] = pd.to_numeric(working["age"], errors="coerce")
    working["gender"] = working["gender"].astype("string")
    working["tenure"] = pd.to_numeric(working["tenure"], errors="coerce")
    working["monthly_charges"] = pd.to_numeric(working["monthly_charges"], errors="coerce")
    working["total_charges"] = pd.to_numeric(working["total_charges"], errors="coerce")
    working["contract"] = working["contract"].astype("string")
    working["internet_service"] = working["internet_service"].astype("string")
    working["payment_method"] = working["payment_method"].astype("string")
    working["churn_target"] = working["churn"].map(normalize_churn_value)

    working = working[working["customer_id"].ne("")]
    working = working[working["churn_target"].notna()].copy()

    if working.empty:
        raise ValueError("Telecom churn table does not contain any valid training rows.")

    unique_targets = set(working["churn_target"].astype(int).tolist())
    if len(unique_targets) < 2:
        raise ValueError("Telecom churn training requires both leave and stay rows.")

    features = working[
        [
            "age",
            "gender",
            "tenure",
            "contract",
            "monthly_charges",
            "total_charges",
            "internet_service",
            "payment_method",
        ]
    ]
    target = working["churn_target"].astype(int)

    preprocessor = ColumnTransformer(
        transformers=[
            (
                "numeric",
                Pipeline(steps=[("imputer", SimpleImputer(strategy="median"))]),
                ["age", "tenure", "monthly_charges", "total_charges"],
            ),
            (
                "categorical",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("encoder", OneHotEncoder(handle_unknown="ignore")),
                    ]
                ),
                ["gender", "contract", "internet_service", "payment_method"],
            ),
        ]
    )
    model = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("classifier", LogisticRegression(max_iter=1000, class_weight="balanced")),
        ]
    )
    model.fit(features, target)

    probabilities = model.predict_proba(features)[:, 1]
    probability_series = pd.Series(probabilities, dtype="float64")
    ranked_probabilities = probability_series.rank(method="first")
    quantile_bands = pd.qcut(ranked_probabilities, q=3, labels=["Low", "Medium", "High"])
    risk_bands = quantile_bands.astype(str).tolist()
    predicted_statuses = [
        "Leave" if band == "High" else "At Risk" if band == "Medium" else "Stay"
        for band in risk_bands
    ]

    predictions = pd.DataFrame(
        {
            "customer_id": working["customer_id"],
            "name": working["name"],
            "age": working["age"],
            "contract": working["contract"],
            "monthly_charges": working["monthly_charges"].round(2),
            "churn_risk": probabilities.round(4),
            "risk_band": risk_bands,
            "predicted_status": predicted_statuses,
        }
    )
    return predictions, model


def upload_parquet(dataframe: pd.DataFrame, bucket_name: str, object_key: str) -> None:
    buffer = io.BytesIO()
    table = pa.Table.from_pandas(dataframe, preserve_index=False)
    pq.write_table(table, buffer)
    buffer.seek(0)
    minio_client().put_object(Bucket=bucket_name, Key=object_key, Body=buffer.getvalue())


def upload_model(model: Pipeline, source_table_name: str, row_count: int) -> None:
    client = minio_client()
    with tempfile.TemporaryDirectory() as temp_dir:
        model_path = Path(temp_dir) / "telecom_churn.joblib"
        joblib.dump(model, model_path)
        client.upload_file(str(model_path), MINIO_MODELS_BUCKET, MODEL_OBJECT_KEY)

    metadata = {
        "model_name": "telecom_churn_logistic_regression",
        "source_table_name": source_table_name,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "training_row_count": row_count,
        "features": [
            "age",
            "gender",
            "tenure",
            "contract",
            "monthly_charges",
            "total_charges",
            "internet_service",
            "payment_method",
        ],
        "target": "churn",
        "prediction_output": f"s3://{MINIO_ANALYTICS_BUCKET}/{PREDICTIONS_OBJECT_KEY}",
    }
    client.put_object(
        Bucket=MINIO_MODELS_BUCKET,
        Key=MODEL_METADATA_OBJECT_KEY,
        Body=json.dumps(metadata, indent=2).encode("utf-8"),
        ContentType="application/json",
    )


def load_model() -> Pipeline:
    client = minio_client()
    buffer = io.BytesIO()
    try:
        client.download_fileobj(MINIO_MODELS_BUCKET, MODEL_OBJECT_KEY, buffer)
    except ClientError as err:
        error_code = err.response.get("Error", {}).get("Code", "")
        if error_code in {"404", "NoSuchKey", "NoSuchBucket", "NotFound"}:
            raise ValueError("Train the telecom model first before uploading prediction data.") from err
        raise
    buffer.seek(0)
    return joblib.load(buffer)


def build_scored_predictions(dataframe: pd.DataFrame, model: Pipeline) -> pd.DataFrame:
    working = dataframe.copy()
    working["customer_id"] = working["customer_id"].astype(str).str.strip()
    working["name"] = working["name"].astype(str).str.strip()
    working["age"] = pd.to_numeric(working["age"], errors="coerce")
    working["gender"] = working["gender"].astype("string")
    working["tenure"] = pd.to_numeric(working["tenure"], errors="coerce")
    working["monthly_charges"] = pd.to_numeric(working["monthly_charges"], errors="coerce")
    working["total_charges"] = pd.to_numeric(working["total_charges"], errors="coerce")
    working["contract"] = working["contract"].astype("string")
    working["internet_service"] = working["internet_service"].astype("string")
    working["payment_method"] = working["payment_method"].astype("string")
    working = working[working["customer_id"].ne("")].copy()

    features = working[
        [
            "age",
            "gender",
            "tenure",
            "contract",
            "monthly_charges",
            "total_charges",
            "internet_service",
            "payment_method",
        ]
    ]
    probabilities = model.predict_proba(features)[:, 1]
    probability_series = pd.Series(probabilities, dtype="float64")
    ranked_probabilities = probability_series.rank(method="first")
    quantile_bands = pd.qcut(ranked_probabilities, q=3, labels=["Low", "Medium", "High"])
    risk_bands = quantile_bands.astype(str).tolist()
    predicted_statuses = [
        "Leave" if band == "High" else "At Risk" if band == "Medium" else "Stay"
        for band in risk_bands
    ]

    return pd.DataFrame(
        {
            "customer_id": working["customer_id"],
            "name": working["name"],
            "age": working["age"],
            "contract": working["contract"],
            "monthly_charges": working["monthly_charges"].round(2),
            "churn_risk": probabilities.round(4),
            "risk_band": risk_bands,
            "predicted_status": predicted_statuses,
        }
    )


def run_training(table_name: str = "telecom_customer_training") -> dict[str, object]:
    ensure_bucket_exists(MINIO_ANALYTICS_BUCKET)
    ensure_bucket_exists(MINIO_MODELS_BUCKET)

    try:
        dataframe = load_table_dataframe(table_name, TRAINING_COLUMNS)
    except LookupError as err:
        return {
            "status": "skipped",
            "reason": str(err),
        }
    except ValueError as err:
        return {
            "status": "skipped",
            "reason": str(err),
        }

    predictions, model = build_predictions(dataframe)
    upload_model(model, table_name, len(predictions))

    return {
        "status": "ok",
        "source_table_name": table_name,
        "training_row_count": len(predictions),
        "model_output": f"s3://{MINIO_MODELS_BUCKET}/{MODEL_OBJECT_KEY}",
    }


def run_prediction(table_name: str = "telecom_customer_data") -> dict[str, object]:
    ensure_bucket_exists(MINIO_ANALYTICS_BUCKET)
    ensure_bucket_exists(MINIO_MODELS_BUCKET)

    try:
        dataframe = load_table_dataframe(table_name, FEATURE_COLUMNS, {"churn"})
        model = load_model()
    except LookupError as err:
        return {
            "status": "skipped",
            "reason": str(err),
        }
    except ValueError as err:
        return {
            "status": "skipped",
            "reason": str(err),
        }

    predictions = build_scored_predictions(dataframe, model)
    upload_parquet(predictions, MINIO_ANALYTICS_BUCKET, PREDICTIONS_OBJECT_KEY)

    return {
        "status": "ok",
        "source_table_name": table_name,
        "prediction_row_count": len(predictions),
        "prediction_output": f"s3://{MINIO_ANALYTICS_BUCKET}/{PREDICTIONS_OBJECT_KEY}",
    }


def main() -> int:
    result = run_training()
    if result["status"] == "ok":
        print(
            "[telecom-churn] Trained telecom model from "
            f"{result['source_table_name']} and stored it at {result['model_output']}"
        )
    else:
        print(f"[telecom-churn] Skipping ML step: {result['reason']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
