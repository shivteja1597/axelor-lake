import os
import re

from dotenv import load_dotenv
from langchain_groq import ChatGroq
from rich.console import Console
from rich.panel import Panel

from tools.db_tools import get_allowed_table_columns, get_allowed_table_counts, run_safe_sql
from tools.guard import REFUSAL, is_project_question
from tools.project_context import PROJECT_CONTEXT
from tools.project_tools import (
    create_api_documentation,
    create_project_flow_documentation,
    format_search_results,
    search_project_files,
)
from tools.rag_tools import format_rag_context, format_rag_sources, has_rag_index, retrieve_project_context
from tools.sql_agent import answer_database_question_dynamically

load_dotenv()

console = Console()

SYSTEM_PROMPT = """
You are an AI assistant only for the Axelor Lakehouse project.

You can answer questions about:
- Axelor lake module
- customer CSV upload flow
- MinIO storage
- Jenkins pipeline
- dbt transformations
- customer risk prediction
- PostgreSQL dashboard tables
- project code and architecture

If the user asks anything unrelated to this project, refuse with:
"I can only answer questions related to this Axelor Lakehouse project."

Be concise, practical, and explain using this project's flow when possible.
Use only the project context and database/query results provided to you.
Do not claim live database facts unless they come from a database tool result.
"""


def build_llm() -> ChatGroq:
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key or api_key == "replace_with_your_groq_api_key":
        raise RuntimeError("Set GROQ_API_KEY in ai-agent/.env before running the agent.")

    return ChatGroq(
        model=os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile"),
        temperature=0,
        api_key=api_key,
    )


def answer_question(question: str) -> str:
    greeting = answer_greeting(question)
    if greeting is not None:
        return greeting

    gratitude = answer_gratitude(question)
    if gratitude is not None:
        return gratitude

    if not is_project_question(question):
        return REFUSAL

    db_answer = answer_database_question(question)
    if db_answer is not None:
        return db_answer

    project_answer = answer_project_tool_question(question)
    if project_answer is not None:
        return project_answer

    rag_answer = answer_with_rag(question)
    if rag_answer is not None:
        return rag_answer

    return answer(build_llm(), question)


def answer_greeting(question: str) -> str | None:
    text = question.strip().lower()
    if text in {"hi", "hii", "hello", "hey", "hey there", "good morning", "good afternoon", "good evening"}:
        return (
            "Hi! I am your Axelor Lakehouse assistant. How can I help you?\n\n"
            "I can help you with:\n"
            "- Customer prediction counts, risk segments, and important customers\n"
            "- PostgreSQL data queries from allowed lakehouse tables\n"
            "- Customer CSV upload flow from Axelor UI to MinIO, Jenkins, ML, and dashboard\n"
            "- Jenkins, dbt, MinIO, PostgreSQL, and Python ML responsibilities\n"
            "- Finding project code files like controllers, services, views, and pipeline logic\n\n"
            "Try asking: \"give me high risk customers count\", "
            "\"show customer contract status summary\", or \"where is Jenkins triggered?\""
        )
    return None


def answer_gratitude(question: str) -> str | None:
    text = normalize_short_text(question)
    gratitude_markers = ("thank you", "thanks", "thank u", "thx")
    if any(marker in text for marker in gratitude_markers):
        return (
            "You're welcome! I am here whenever you need help with the Axelor Lakehouse project, "
            "customer predictions, PostgreSQL data, Jenkins flow, MinIO storage, or code locations."
        )
    return None


def normalize_short_text(text: str) -> str:
    return " ".join(text.strip().lower().replace(",", " ").replace(".", " ").replace("!", " ").split())


def answer(llm: ChatGroq, question: str) -> str:
    response = llm.invoke(
        [
            ("system", SYSTEM_PROMPT + "\n\n" + PROJECT_CONTEXT),
            ("human", question),
        ]
    )
    return response.content


def answer_with_rag(question: str) -> str | None:
    if not has_rag_index():
        return None

    chunks = retrieve_project_context(question)
    if not chunks:
        return None

    context = format_rag_context(chunks)
    sources = format_rag_sources(chunks)
    response = build_llm().invoke(
        [
            (
                "system",
                SYSTEM_PROMPT
                + "\n\nAnswer using only the retrieved project context below. "
                + "If the context is not enough, say what is missing instead of guessing.\n\n"
                + context,
            ),
            ("human", question),
        ]
    )
    return f"{response.content}\n\nSources:\n{sources}"


def answer_database_question(question: str) -> str | None:
    text = question.lower()

    sql_query = extract_select_query(question)
    if sql_query:
        result = run_safe_sql(sql_query)
        if isinstance(result, str):
            return result
        return format_query_result(result)

    if should_use_dynamic_database_agent(text):
        dynamic_answer = answer_database_question_dynamically(build_llm(), question)
        if dynamic_answer:
            return dynamic_answer

    if "customer prediction" in text and any(
        phrase in text for phrase in ("how many", "count", "rows", "stored")
    ):
        result = run_safe_sql("SELECT COUNT(*) AS total FROM lake_lake_customer_prediction")
        if isinstance(result, str):
            return result
        return f"PostgreSQL has {result['rows'][0][0]} customer prediction rows."

    if is_top_churn_risk_customer_question(text):
        return answer_top_churn_risk_customers(text)

    if "salary" in text and "customer" in text:
        result = run_safe_sql(
            """
            SELECT ROUND(AVG(current_rmr), 2) AS avg_customer_current_rmr
            FROM lake_lake_customer_prediction
            """
        )
        if isinstance(result, str):
            return result
        value = result["rows"][0][0]
        return (
            "Customer prediction data does not have a salary column. "
            f"The closest customer money field is current_rmr; its average is {value}."
        )

    if "salary" in text and ("employee" in text or "average" in text or "avg" in text):
        result = run_safe_sql("SELECT ROUND(AVG(salary), 2) AS avg_salary FROM lake_lake_employee")
        if isinstance(result, str):
            return result
        return f"The average employee salary is {result['rows'][0][0]}."

    if "customer" in text and "contract" in text and "status" in text:
        return answer_customer_contract_status_question(question, text)

    if "customer" in text and any(term in text for term in ("important", "priority", "actionable")):
        return answer_important_customer_question(text)

    if ("sync" in text or "status" in text) and "customer" in text:
        result = run_safe_sql(
            """
            SELECT table_name, sync_status, sync_message, last_sync_at
            FROM lake_lakehouse_table
            WHERE table_name = 'customer_profile'
            """
        )
        if isinstance(result, str):
            return result
        if not result.get("rows"):
            return (
                "No lakehouse metadata row exists for customer_profile right now. "
                "The prediction table can still contain published rows."
            )
        return format_query_result(result)

    if "table" in text and any(phrase in text for phrase in ("count", "counts", "how many")):
        counts = get_allowed_table_counts()
        return "\n".join(f"{table}: {count}" for table, count in counts.items())

    if "columns" in text or "schema" in text:
        table_name = extract_allowed_table_name(text)
        if table_name:
            columns = get_allowed_table_columns(table_name)
            if isinstance(columns, str):
                return columns
            return f"{table_name} columns:\n" + "\n".join(f"- {column}" for column in columns)

    risk_segment = extract_risk_segment(text)
    if risk_segment and "customer" in text:
        return answer_risk_segment_question(text, risk_segment)

    if "sample" in text and "customer" in text:
        result = run_safe_sql(
            """
            SELECT account_id, customer_name, risk_segment, churn_risk_percentage
            FROM lake_lake_customer_prediction
            ORDER BY id
            LIMIT 10
            """
        )
        if isinstance(result, str):
            return result
        return format_query_result(result)

    if "risk segment" in text or "segment summary" in text or "segmentation" in text:
        result = run_safe_sql(
            """
            SELECT risk_segment, COUNT(*) AS customers,
                   ROUND(AVG(churn_risk_percentage), 2) AS avg_churn_risk
            FROM lake_lake_customer_prediction
            GROUP BY risk_segment
            ORDER BY customers DESC
            """
        )
        if isinstance(result, str):
            return result
        return format_query_result(result)

    if looks_like_database_question(text):
        return (
            "I did not run that because it needs a safe database route. "
            "Ask for a supported metric like prediction row count, risk segment summary, "
            "sample customer predictions, table schema, or provide an explicit SELECT query "
            "against an allowed lake table."
        )

    return None


def answer_top_churn_risk_customers(text: str) -> str:
    limit = extract_limit(text, default=10)
    result = run_safe_sql(
        f"""
        SELECT account_id, customer_name, contract_status, current_rmr,
               risk_segment, churn_risk_percentage
        FROM lake_lake_customer_prediction
        ORDER BY churn_risk_percentage DESC
        LIMIT {limit}
        """
    )
    if isinstance(result, str):
        return result
    return f"Top {limit} customers by churn risk:\n" + format_query_result(result)


def is_top_churn_risk_customer_question(text: str) -> bool:
    return (
        "customer" in text
        and ("churn" in text or "risk" in text)
        and any(term in text for term in ("top", "highest", "most"))
    )


def answer_important_customer_question(text: str) -> str:
    if is_count_question(text):
        result = run_safe_sql(
            """
            SELECT COUNT(*) AS important_customers
            FROM lake_lake_customer_prediction
            WHERE LOWER(risk_segment) LIKE 'high%'
            """
        )
        if isinstance(result, str):
            return result
        return (
            f"There are {result['rows'][0][0]} important customers. "
            "Here, important means customers in the High Risk segment who should be prioritized."
        )

    result = run_safe_sql(
        """
        SELECT account_id, customer_name, contract_status, current_rmr,
               risk_segment, churn_risk_percentage
        FROM lake_lake_customer_prediction
        WHERE LOWER(risk_segment) LIKE 'high%'
        ORDER BY churn_risk_percentage DESC, current_rmr DESC
        LIMIT 10
        """
    )
    if isinstance(result, str):
        return result
    return (
        "Important customers are treated as High Risk customers with the highest churn risk. "
        "Top 10:\n"
        + format_query_result(result)
    )


def answer_customer_contract_status_question(question: str, text: str) -> str:
    status = extract_contract_status(question)
    if not status:
        result = run_safe_sql(
            """
            SELECT contract_status, COUNT(*) AS customers
            FROM lake_lake_customer_prediction
            GROUP BY contract_status
            ORDER BY customers DESC
            """
        )
        if isinstance(result, str):
            return result
        return format_query_result(result)

    escaped_status = status.replace("'", "''")
    if is_count_question(text):
        result = run_safe_sql(
            f"""
            SELECT COUNT(*) AS customers
            FROM lake_lake_customer_prediction
            WHERE LOWER(contract_status) = LOWER('{escaped_status}')
            """
        )
        if isinstance(result, str):
            return result
        return f"There are {result['rows'][0][0]} customers whose contract status is {status}."

    result = run_safe_sql(
        f"""
        SELECT account_id, customer_name, contract_status, risk_segment, churn_risk_percentage
        FROM lake_lake_customer_prediction
        WHERE LOWER(contract_status) = LOWER('{escaped_status}')
        ORDER BY id
        LIMIT 10
        """
    )
    if isinstance(result, str):
        return result
    return format_query_result(result)


def extract_contract_status(question: str) -> str | None:
    text = question.lower()
    known_statuses = {
        "active": "Active",
        "cancelled": "Cancelled",
        "canceled": "Cancelled",
        "expired": "Expired",
        "suspended": "Suspended",
        "pending": "Pending",
    }
    for keyword, status in known_statuses.items():
        if keyword in text:
            return status

    match = re.search(
        r"contract\s+status\s+(?:is|=|equals|as)\s+([a-z][a-z0-9 _-]*)",
        question,
        flags=re.IGNORECASE,
    )
    if not match:
        return None

    value = match.group(1).strip(" .?\"'")
    return value.title() if value else None


def answer_risk_segment_question(text: str, risk_segment: str) -> str:
    if is_count_question(text):
        result = run_safe_sql(
            f"""
            SELECT COUNT(*) AS {risk_segment}_risk_customers
            FROM lake_lake_customer_prediction
            WHERE LOWER(risk_segment) LIKE '{risk_segment}%'
            """
        )
        if isinstance(result, str):
            return result
        return f"There are {result['rows'][0][0]} {risk_segment} risk customers in PostgreSQL."

    if any(phrase in text for phrase in ("percentage", "percent", "%", "average", "avg")):
        result = run_safe_sql(
            f"""
            SELECT ROUND(AVG(churn_risk_percentage), 2) AS avg_churn_risk_percentage
            FROM lake_lake_customer_prediction
            WHERE LOWER(risk_segment) LIKE '{risk_segment}%'
            """
        )
        if isinstance(result, str):
            return result
        value = result["rows"][0][0]
        return f"The average churn risk percentage for {risk_segment} risk customers is {value}%."

    limit = extract_limit(text, default=10)
    result = run_safe_sql(
        f"""
        SELECT account_id, customer_name, churn_risk_percentage, risk_segment
        FROM lake_lake_customer_prediction
        WHERE LOWER(risk_segment) LIKE '{risk_segment}%'
        ORDER BY churn_risk_percentage DESC
        LIMIT {limit}
        """
    )
    if isinstance(result, str):
        return result
    return format_query_result(result)


def extract_risk_segment(text: str) -> str | None:
    for segment in ("high", "medium", "low"):
        if f"{segment} risk" in text or f"{segment}-risk" in text:
            return segment
    return None


def is_count_question(text: str) -> bool:
    return any(phrase in text for phrase in ("count", "how many", "total", "number"))


def should_use_dynamic_database_agent(text: str) -> bool:
    project_lookup_terms = (
        "code",
        "controller",
        "documentation",
        "endpoint",
        "file",
        "flow",
        "java",
        "jenkins",
        "minio",
        "publish endpoint",
        "python",
        "service",
        "upload flow",
        "where is",
        "which file",
    )
    if any(term in text for term in project_lookup_terms):
        return False

    database_intent_terms = {
        "average",
        "avg",
        "by",
        "count",
        "customer",
        "customers",
        "data",
        "database",
        "db",
        "group",
        "highest",
        "how many",
        "list",
        "records",
        "risk",
        "rows",
        "segment",
        "show",
        "state",
        "status",
        "sum",
        "table",
        "top",
        "total",
    }
    return any(term in text for term in database_intent_terms)


def extract_limit(text: str, default: int = 10, maximum: int = 50) -> int:
    match = re.search(r"\b(?:top|limit|first)\s+(\d+)\b", text)
    if not match:
        return default
    return max(1, min(int(match.group(1)), maximum))


def looks_like_database_question(text: str) -> bool:
    database_terms = {
        "average",
        "avg",
        "count",
        "data",
        "database",
        "db",
        "query",
        "record",
        "records",
        "row",
        "rows",
        "salary",
        "sum",
        "table",
        "total",
    }
    return any(term in text for term in database_terms)


def answer_project_tool_question(question: str) -> str | None:
    text = question.lower()

    if "create" in text and "documentation" in text:
        if "flow" in text or "project" in text:
            return create_project_flow_documentation()

        target_name = extract_documentation_target(question)
        if target_name:
            return create_api_documentation(target_name)
        return (
            "Please specify what documentation to create, for example: "
            "\"Create API documentation for LakehouseService\" or "
            "\"Create documentation which explains flow of the project\"."
        )

    if any(phrase in text for phrase in ("find code", "where is", "which file", "show file")):
        results = search_project_files(question)
        return format_search_results(results)

    if "full flow" in text or ("upload" in text and "flow" in text):
        return (
            "Customer CSV flow:\n"
            "1. Axelor UI uploads customer_profile CSV.\n"
            "2. LakehouseController calls LakehouseServiceImpl.\n"
            "3. Java stores the raw CSV in MinIO lake-raw/customer_profile/.\n"
            "4. Java triggers Jenkins job open-suite-lake-pipeline with TABLE_NAME=customer_profile.\n"
            "5. Jenkins runs dbt customer transformation.\n"
            "6. Jenkins runs python-ml/customer_risk.py for ML prediction.\n"
            "7. Python writes customer_predictions.parquet to MinIO lake-analytics.\n"
            "8. Jenkins calls Axelor publish endpoint under /axelor-erp/ws/lake/customer-predictions/publish.\n"
            "9. Java reads the parquet through PgDuckDB and inserts rows into lake_lake_customer_prediction.\n"
            "10. Dashboard reads prediction summary/table data from PostgreSQL."
        )

    return None


def extract_documentation_target(question: str) -> str | None:
    match = re.search(
        r"(?:for|of)\s+([A-Za-z_][A-Za-z0-9_]*(?:Service|Controller|Repository|Endpoint))\b",
        question,
        flags=re.IGNORECASE,
    )
    if match:
        return match.group(1)
    return None


def extract_allowed_table_name(text: str) -> str | None:
    match = re.search(r"lake_[a-z0-9_]+", text)
    return match.group(0) if match else None


def extract_select_query(question: str) -> str | None:
    stripped = question.strip()
    lowered = stripped.lower()
    if lowered.startswith("select "):
        return stripped
    if lowered.startswith("sql:"):
        candidate = stripped[4:].strip()
        return candidate if candidate.lower().startswith("select ") else None
    return None


def format_query_result(result: dict) -> str:
    rows = result.get("rows", [])
    columns = result.get("columns", [])
    if not rows:
        return "No rows found."

    lines = [" | ".join(columns)]
    lines.append(" | ".join("-" for _ in columns))
    for row in rows:
        lines.append(" | ".join("" if value is None else str(value) for value in row))
    return "\n".join(lines)


def main() -> None:
    llm = build_llm()
    console.print(Panel.fit("Axelor Lakehouse Agent is ready. Type 'exit' to stop."))

    while True:
        question = console.input("[bold cyan]Ask:[/bold cyan] ").strip()
        if question.lower() in {"exit", "quit"}:
            break
        if not question:
            continue

        try:
            console.print(Panel(answer_question(question), title="Agent"))
        except Exception as exc:
            console.print(Panel(str(exc), title="Error", style="red"))


if __name__ == "__main__":
    main()
