import os
import sys
import tempfile
import json
from datetime import datetime, timezone
from pathlib import Path

import boto3
import duckdb
import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")
MINIO_CURATED_BUCKET = os.getenv("MINIO_CURATED_BUCKET", "lake-curated")
MINIO_ANALYTICS_BUCKET = os.getenv("MINIO_ANALYTICS_BUCKET", "lake-analytics")
MINIO_MODELS_BUCKET = os.getenv("MINIO_MODELS_BUCKET", "lake-models")
FEATURE_OBJECT = os.getenv(
    "CUSTOMER_FEATURE_OBJECT", f"s3://{MINIO_CURATED_BUCKET}/customer_profile_features.parquet"
)
PREDICTIONS_OBJECT = os.getenv(
    "CUSTOMER_PREDICTIONS_OBJECT", f"s3://{MINIO_ANALYTICS_BUCKET}/customer_predictions.parquet"
)
SEGMENTS_OBJECT = os.getenv(
    "CUSTOMER_SEGMENTS_OBJECT", f"s3://{MINIO_ANALYTICS_BUCKET}/customer_segments.parquet"
)
MODEL_OBJECT_KEY = os.getenv(
    "CUSTOMER_MODEL_OBJECT_KEY", "customer-risk/logistic_regression.joblib"
)

NUMERIC_FEATURES = [
    "tenure_in_months",
    "days_left_in_recent_contract",
    "current_rmr",
    "fico_score",
    "customer_initial_contract_term",
    "remaining_life_time",
    "projected_rmr_churn",
    "acv",
    "cost_to_serve",
    "rmr_difference_from_company_avg",
    "area_attrition",
]

CATEGORICAL_FEATURES = [
    "plan_type",
    "billing_frequency",
    "payment_mode",
    "property_type",
    "acv_segment",
]

IDENTITY_COLUMNS = [
    "account_id",
    "site_id",
    "customer_name",
    "state",
    "plan_type",
    "current_rmr",
    "customer_segment_bucket",
]

TARGET_COLUMN = "contract_status"
TARGET_MAPPING = {"active": 0, "cancelled": 1}


def configure_connection(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute("INSTALL httpfs;")
    connection.execute("LOAD httpfs;")
    endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    connection.execute(f"SET s3_endpoint='{endpoint}';")
    connection.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
    connection.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
    connection.execute(f"SET s3_region='{MINIO_REGION}';")
    connection.execute("SET s3_use_ssl=false;")
    connection.execute("SET s3_url_style='path';")


def load_feature_data(connection: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return connection.execute(f"SELECT * FROM read_parquet('{FEATURE_OBJECT}')").fetchdf()


def prepare_training_frame(
    features: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame]:
    missing_columns = [
        column
        for column in IDENTITY_COLUMNS + NUMERIC_FEATURES + CATEGORICAL_FEATURES + [TARGET_COLUMN]
        if column not in features.columns
    ]
    if missing_columns:
        raise ValueError(
            "Customer feature data is missing required columns for training: "
            + ", ".join(missing_columns)
        )

    training_frame = features.copy()
    training_frame[TARGET_COLUMN] = (
        training_frame[TARGET_COLUMN].fillna("").astype(str).str.strip().str.lower()
    )
    training_frame = training_frame[training_frame[TARGET_COLUMN].isin(TARGET_MAPPING.keys())].copy()
    if training_frame.empty:
        raise ValueError("No training rows found after mapping contract_status to Active/Cancelled.")

    training_frame["customer_name"] = training_frame["customer_name"].fillna("").astype(str).str.strip()
    training_frame["account_id"] = training_frame["account_id"].fillna("").astype(str).str.strip()
    for column in NUMERIC_FEATURES:
        training_frame[column] = pd.to_numeric(training_frame[column], errors="coerce")
    for column in CATEGORICAL_FEATURES:
        training_frame[column] = training_frame[column].fillna("").astype(str).str.strip()

    target = training_frame[TARGET_COLUMN].map(TARGET_MAPPING).astype(int)
    model_input = training_frame[NUMERIC_FEATURES + CATEGORICAL_FEATURES]
    return training_frame, target, model_input


def train_model(model_input: pd.DataFrame, target: pd.Series) -> Pipeline:
    numeric_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )
    categorical_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("encoder", OneHotEncoder(handle_unknown="ignore")),
        ]
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("numeric", numeric_pipeline, NUMERIC_FEATURES),
            ("categorical", categorical_pipeline, CATEGORICAL_FEATURES),
        ]
    )

    model = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("classifier", LogisticRegression(max_iter=1000, class_weight="balanced")),
        ]
    )
    model.fit(model_input, target)
    return model


def allocate_risk_segment_counts(total_rows: int) -> tuple[int, int, int]:
    if total_rows <= 0:
        return 0, 0, 0

    high_count = int(round(total_rows * 0.30))
    medium_count = int(round(total_rows * 0.40))
    low_count = total_rows - high_count - medium_count

    if low_count < 0:
        medium_count = max(0, medium_count + low_count)
        low_count = 0

    assigned = high_count + medium_count + low_count
    if assigned < total_rows:
        low_count += total_rows - assigned

    return high_count, medium_count, low_count


def assign_risk_segments(predictions: pd.DataFrame) -> pd.DataFrame:
    ranked = predictions.sort_values(
        by=["churn_risk_percentage", "current_rmr", "account_id"],
        ascending=[False, False, True],
        kind="mergesort",
    ).reset_index()

    high_count, medium_count, _ = allocate_risk_segment_counts(len(ranked))
    ranked["risk_segment"] = "Low Risk"
    if high_count > 0:
        ranked.loc[: high_count - 1, "risk_segment"] = "High Risk"
    if medium_count > 0:
        ranked.loc[high_count : high_count + medium_count - 1, "risk_segment"] = "Medium Risk"

    return ranked.sort_values("index").drop(columns=["index"])


def build_predictions_frame(
    training_frame: pd.DataFrame, model_input: pd.DataFrame, model: Pipeline
) -> pd.DataFrame:
    probabilities = model.predict_proba(model_input)[:, 1] * 100.0
    current_rmr_median = training_frame["current_rmr"].median()
    if pd.isna(current_rmr_median):
        current_rmr_median = 0.0
    predictions = pd.DataFrame(
        {
            "account_id": training_frame["account_id"],
            "site_id": training_frame["site_id"].fillna("").astype(str).str.strip(),
            "customer_name": training_frame["customer_name"],
            "state": training_frame["state"].fillna("").astype(str).str.strip(),
            "contract_status": training_frame[TARGET_COLUMN].str.title(),
            "plan_type": training_frame["plan_type"],
            "current_rmr": training_frame["current_rmr"].fillna(current_rmr_median).round(2),
            "churn_risk_percentage": probabilities.round(2),
        }
    )
    predictions["customer_segment_bucket"] = training_frame["customer_segment_bucket"].fillna(
        "Review Required"
    )
    return assign_risk_segments(predictions)


def write_parquet_outputs(
    connection: duckdb.DuckDBPyConnection, predictions: pd.DataFrame
) -> None:
    prediction_export = predictions[
        [
            "account_id",
            "site_id",
            "customer_name",
            "state",
            "contract_status",
            "plan_type",
            "current_rmr",
            "customer_segment_bucket",
            "churn_risk_percentage",
            "risk_segment",
        ]
    ].copy()
    connection.register("customer_predictions_df", prediction_export)
    connection.execute(
        f"""
        COPY customer_predictions_df
        TO '{PREDICTIONS_OBJECT}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE)
        """
    )

    segments = (
        predictions.groupby(["customer_segment_bucket", "risk_segment"], dropna=False)
        .size()
        .reset_index(name="customer_count")
        .sort_values(["customer_segment_bucket", "risk_segment"])
    )
    connection.register("customer_segments_df", segments)
    connection.execute(
        f"""
        COPY customer_segments_df
        TO '{SEGMENTS_OBJECT}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE)
        """
    )


def upload_model(model: Pipeline) -> None:
    endpoint = MINIO_ENDPOINT.rstrip("/")
    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name=MINIO_REGION,
    )
    with tempfile.TemporaryDirectory() as tmp_dir:
        model_path = Path(tmp_dir) / "customer_risk_logistic_regression.joblib"
        joblib.dump(model, model_path)
        s3_client.upload_file(str(model_path), MINIO_MODELS_BUCKET, MODEL_OBJECT_KEY)


def run_workflow(table_name: str = "customer_profile") -> dict[str, object]:
    connection = duckdb.connect(database=":memory:")
    configure_connection(connection)
    features = load_feature_data(connection)
    training_frame, target, model_input = prepare_training_frame(features)
    model = train_model(model_input, target)
    predictions = build_predictions_frame(training_frame, model_input, model)
    write_parquet_outputs(connection, predictions)
    upload_model(model)
    return {
        "table_name": table_name,
        "training_row_count": len(training_frame),
        "prediction_row_count": len(predictions),
        "prediction_output": PREDICTIONS_OBJECT,
        "segment_output": SEGMENTS_OBJECT,
        "model_output": f"s3://{MINIO_MODELS_BUCKET}/{MODEL_OBJECT_KEY}",
    }


def main() -> int:
    result = run_workflow()
    print(
        "[customer-risk] Trained logistic regression from contract_status and generated "
        f"{result['prediction_row_count']} predictions at {result['prediction_output']}, "
        f"segments at {result['segment_output']}, "
        f"model at {result['model_output']}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
