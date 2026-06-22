import json
import re

from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage
from langchain_core.tools import tool

from tools.db_tools import describe_allowed_table, list_allowed_tables, run_safe_sql

SQL_AGENT_SYSTEM_PROMPT = """
You are an AI data agent for the Axelor Lakehouse project.

Use the database tools to answer factual questions from the Axelor PostgreSQL database.
You must only use read-only SELECT queries.
You must only query tables returned by list_lakehouse_tables.
Before writing SQL, inspect table names and schema when needed.
For factual data questions, you must call query_lakehouse_database and return the actual data.
Do not answer with only a SQL query or instructions for the user to run a query.

Important table guidance:
- Customer prediction/churn data is in lake_lake_customer_prediction.
- Sync metadata is in lake_lakehouse_table.
- Employee data is in lake_lake_employee.

For lake_lake_customer_prediction, useful columns include:
- account_id
- customer_name
- contract_status
- current_rmr
- risk_segment
- churn_risk_percentage
- state
- plan_type
- customer_segment_bucket

Never invent table or column names.
If a requested metric is not represented by the allowed schema, say that clearly.
Return a concise human answer with the data result and include the SQL you used.
"""


@tool
def list_lakehouse_tables() -> str:
    """List the PostgreSQL lakehouse tables that the agent is allowed to query."""
    return json.dumps(list_allowed_tables())


@tool
def describe_lakehouse_table(table_name: str) -> str:
    """Describe columns and data types for one allowed lakehouse table."""
    result = describe_allowed_table(table_name)
    return result if isinstance(result, str) else json.dumps(result)


@tool
def query_lakehouse_database(sql: str) -> str:
    """Run a validated read-only SELECT query against allowed lakehouse tables."""
    result = run_safe_sql(sql)
    return result if isinstance(result, str) else json.dumps(result, default=str)


SQL_TOOLS = [list_lakehouse_tables, describe_lakehouse_table, query_lakehouse_database]


def answer_database_question_dynamically(llm, question: str, max_steps: int = 5) -> str | None:
    tool_by_name = {tool_item.name: tool_item for tool_item in SQL_TOOLS}
    llm_with_tools = llm.bind_tools(SQL_TOOLS)
    messages = [SystemMessage(content=SQL_AGENT_SYSTEM_PROMPT), HumanMessage(content=question)]
    query_was_executed = False

    for _ in range(max_steps):
        response = llm_with_tools.invoke(messages)
        messages.append(response)

        tool_calls = getattr(response, "tool_calls", None) or []
        if not tool_calls:
            content = getattr(response, "content", "")
            if not content:
                return None
            if not query_was_executed or should_force_execute_final_sql(content):
                executed_answer = execute_select_from_model_answer(content)
                if executed_answer:
                    return executed_answer
            return content

        for tool_call in tool_calls:
            tool_name = tool_call["name"]
            selected_tool = tool_by_name.get(tool_name)
            if selected_tool is None:
                tool_result = f"Unknown tool: {tool_name}"
            else:
                if tool_name == "query_lakehouse_database":
                    query_was_executed = True
                tool_result = selected_tool.invoke(tool_call.get("args", {}))

            messages.append(
                ToolMessage(
                    content=str(tool_result),
                    tool_call_id=tool_call["id"],
                )
            )

    final_response = llm.invoke(
        messages
        + [
            HumanMessage(
                content=(
                    "Summarize the database tool results already provided. "
                    "If no query result was returned, say what is missing."
                )
            )
        ]
    )
    return final_response.content if final_response.content else None


def execute_select_from_model_answer(content: str) -> str | None:
    sql = extract_select_sql(content)
    if not sql:
        return None

    result = run_safe_sql(sql)
    if isinstance(result, str):
        return result

    return (
        "I executed the SQL and found this data:\n"
        + format_query_result(result)
        + f"\n\nSQL used: {sql}"
    )


def should_force_execute_final_sql(content: str) -> bool:
    lowered = content.lower()
    negative_markers = (
        "unable to retrieve",
        "did not return any results",
        "did not return any rows",
        "no results",
        "not available",
        "please try again",
        "will return",
        "actual result will depend",
        "you can run",
    )
    return "select" in lowered and any(marker in lowered for marker in negative_markers)


def extract_select_sql(content: str) -> str | None:
    fenced_match = re.search(r"```sql\s*(select\b.*?)```", content, flags=re.IGNORECASE | re.DOTALL)
    if fenced_match:
        return fenced_match.group(1).strip().rstrip(";")

    inline_match = re.search(
        r"(select\b.*?)(?:\n\n|$)",
        content,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if inline_match:
        return inline_match.group(1).strip().rstrip(";")

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
