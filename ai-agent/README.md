# Axelor Lakehouse AI Agent

Project-scoped LangChain + Groq assistant for the Axelor Lakehouse module.

## Setup

From the repository root:

```powershell
cd ai-agent
.\setup.ps1
```

Then edit `.env` and set:

```env
GROQ_API_KEY=your_groq_api_key
```

Run:

```powershell
python app.py
```

To verify Groq and PostgreSQL connectivity:

```powershell
python check_env.py
```

Build the local RAG index:

```powershell
python index_project.py
```

Run this again whenever lake module code, generated docs, dbt files, or Python ML files change.

If PowerShell blocks script execution, run this once in the same terminal:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

## Scope

The agent is intentionally project-only. It should answer Axelor Lakehouse questions and refuse unrelated prompts.

Allowed topics include:

- Customer CSV upload
- MinIO storage
- Jenkins pipeline
- dbt and ML prediction flow
- PostgreSQL prediction tables
- Dashboard data flow
- Axelor lake module code
- RAG answers from indexed project files and generated docs
- Dynamic read-only SQL answers through Groq tool calling

## Example prompts

```text
Explain the full flow when I upload customer csv.
How many customer prediction rows are stored in PostgreSQL?
Give customer risk segment summary.
Show sample customer predictions.
Show schema for lake_lake_customer_prediction.
Show customer sync status.
Where is the customer publish endpoint?
Explain LakehouseServiceImpl using project files.
How does Jenkins publish predictions back to Axelor?
Show top 10 customers by churn risk.
Show count of customers by risk segment.
What is average churn risk percentage by state?
```

You can also run read-only SQL against approved lake tables:

```text
SELECT COUNT(*) FROM lake_lake_customer_prediction
sql: SELECT risk_segment, COUNT(*) FROM lake_lake_customer_prediction GROUP BY risk_segment
```

The agent blocks write SQL and unrelated questions.

## Dynamic SQL Agent

For database questions, the agent can use Groq tool calling with these safe tools:

- `list_lakehouse_tables`
- `describe_lakehouse_table`
- `query_lakehouse_database`

The SQL tool only allows `SELECT` queries and only against approved lakehouse tables.
