PROJECT_KEYWORDS = {
    "axelor",
    "lake",
    "lakehouse",
    "minio",
    "jenkins",
    "customer",
    "prediction",
    "predictions",
    "postgres",
    "postgresql",
    "dashboard",
    "csv",
    "dbt",
    "duckdb",
    "pgduckdb",
    "pipeline",
    "churn",
    "risk",
    "java",
    "controller",
    "service",
    "iceberg",
    "parquet",
    "records",
    "rows",
    "tables",
    "sync",
    "upload",
    "flow",
    "model",
    "ml",
    "database",
    "db",
    "sql",
}


def is_project_question(question: str) -> bool:
    text = question.lower()
    return any(keyword in text for keyword in PROJECT_KEYWORDS)


REFUSAL = "I can only answer questions related to this Axelor Lakehouse project."
