import os
import re
from typing import Any

import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()

ALLOWED_TABLES = {
    "lake_lake_customer_prediction",
    "lake_lakehouse_table",
    "lake_lake_employee",
    "lake_lake_department",
    "lake_lake_role",
    "lake_lake_status",
}

BLOCKED_SQL = {"insert", "update", "delete", "drop", "alter", "truncate", "create"}


def run_safe_sql(query: str) -> dict[str, Any] | str:
    validation_error = validate_select_sql(query)
    if validation_error:
        return validation_error

    max_rows = int(os.getenv("MAX_DB_ROWS", "50"))
    safe_query = ensure_limit(query, max_rows)

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(safe_query)
            rows = cursor.fetchmany(max_rows)
            columns = [column.name for column in cursor.description]

    return {"columns": columns, "rows": rows}


def validate_select_sql(query: str) -> str | None:
    normalized = normalize_sql(query)
    if not normalized.startswith("select"):
        return "Only SELECT queries are allowed."

    if any(re.search(rf"\b{keyword}\b", normalized) for keyword in BLOCKED_SQL):
        return "Write or schema-changing SQL is not allowed."

    referenced_tables = extract_referenced_tables(normalized)
    if not referenced_tables:
        return "That table is not available to the Axelor Lakehouse agent."

    blocked_tables = sorted(referenced_tables - ALLOWED_TABLES)
    if blocked_tables:
        return "Only approved lakehouse tables can be queried."

    return None


def normalize_sql(query: str) -> str:
    return re.sub(r"\s+", " ", query.strip().rstrip(";")).lower()


def extract_referenced_tables(normalized_sql: str) -> set[str]:
    matches = re.findall(r"\b(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_]*)", normalized_sql)
    return {match.lower() for match in matches}


def ensure_limit(query: str, max_rows: int) -> str:
    if re.search(r"\blimit\s+\d+\b", query, flags=re.IGNORECASE):
        return query
    return f"{query.rstrip().rstrip(';')} LIMIT {max_rows}"


def get_connection():
    return psycopg2.connect(
        host=os.getenv("AXELOR_DB_HOST"),
        port=os.getenv("AXELOR_DB_PORT"),
        dbname=os.getenv("AXELOR_DB_NAME"),
        user=os.getenv("AXELOR_DB_USER"),
        password=os.getenv("AXELOR_DB_PASSWORD"),
    )


def list_allowed_tables() -> list[str]:
    return sorted(ALLOWED_TABLES)


def describe_allowed_table(table_name: str) -> list[dict[str, str]] | str:
    normalized = table_name.strip().lower()
    if normalized not in ALLOWED_TABLES:
        return "That table is not available to the Axelor Lakehouse agent."

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = %s
                ORDER BY ordinal_position
                """,
                (normalized,),
            )
            return [{"column": row[0], "type": row[1]} for row in cursor.fetchall()]


def get_allowed_table_counts() -> dict[str, int]:
    counts: dict[str, int] = {}
    with get_connection() as connection:
        with connection.cursor() as cursor:
            for table_name in sorted(ALLOWED_TABLES):
                cursor.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name))
                )
                counts[table_name] = cursor.fetchone()[0]
    return counts


def get_allowed_table_columns(table_name: str) -> list[str] | str:
    normalized = table_name.strip().lower()
    if normalized not in ALLOWED_TABLES:
        return "That table is not available to the Axelor Lakehouse agent."

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = %s
                ORDER BY ordinal_position
                """,
                (normalized,),
            )
            return [row[0] for row in cursor.fetchall()]
