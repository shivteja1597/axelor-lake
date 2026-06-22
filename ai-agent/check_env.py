import os

import psycopg2
from dotenv import load_dotenv
from langchain_groq import ChatGroq
from rich.console import Console
from rich.table import Table

load_dotenv()

console = Console()


def check_groq() -> tuple[str, str]:
    api_key = os.getenv("GROQ_API_KEY", "").strip()
    model = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile").strip()
    if not api_key or api_key == "replace_with_your_groq_api_key":
        return "Groq", "missing GROQ_API_KEY"

    llm = ChatGroq(model=model, temperature=0, api_key=api_key)
    response = llm.invoke("Reply with exactly: ok")
    if response.content.strip().lower() != "ok":
      return "Groq", f"unexpected response: {response.content!r}"
    return "Groq", "ok"


def check_database() -> tuple[str, str]:
    required = [
        "AXELOR_DB_HOST",
        "AXELOR_DB_PORT",
        "AXELOR_DB_NAME",
        "AXELOR_DB_USER",
        "AXELOR_DB_PASSWORD",
    ]
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        return "PostgreSQL", "missing " + ", ".join(missing)

    with psycopg2.connect(
        host=os.getenv("AXELOR_DB_HOST"),
        port=os.getenv("AXELOR_DB_PORT"),
        dbname=os.getenv("AXELOR_DB_NAME"),
        user=os.getenv("AXELOR_DB_USER"),
        password=os.getenv("AXELOR_DB_PASSWORD"),
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM lake_lake_customer_prediction")
            count = cursor.fetchone()[0]

    return "PostgreSQL", f"ok, customer prediction rows: {count}"


def main() -> None:
    table = Table(title="Axelor Lakehouse Agent Environment")
    table.add_column("Check")
    table.add_column("Status")

    for name, status in [check_groq(), check_database()]:
        table.add_row(name, status)

    console.print(table)


if __name__ == "__main__":
    main()

