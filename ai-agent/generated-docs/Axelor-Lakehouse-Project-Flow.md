# Axelor Lakehouse Project Flow

## Overview
This project is an Axelor lakehouse module where Java/Axelor handles the application flow, MinIO stores lakehouse files, Jenkins runs the processing pipeline, Python performs customer risk ML, and PostgreSQL stores published prediction data for dashboards.

## End-to-End Customer CSV Flow
1. User opens Axelor UI menu `Lake > Upload Customer Data`.
2. User uploads a customer CSV for dataset `customer_profile`.
3. `LakehouseController` receives the UI action.
4. `LakehouseServiceImpl` validates and uploads the raw CSV to MinIO.
5. Raw files are stored under MinIO bucket/path like `lake-raw/customer_profile/`.
6. Java triggers Jenkins job `open-suite-lake-pipeline` with `TABLE_NAME=customer_profile`.
7. Jenkins detects dataset type as `customer_profile`.
8. Jenkins runs dbt transformation for customer staging.
9. Jenkins runs `python-ml/customer_risk.py` for ML prediction.
10. Python writes `customer_predictions.parquet` to MinIO analytics storage.
11. Jenkins calls Axelor publish endpoint `/axelor-erp/ws/lake/customer-predictions/publish`.
12. Java reads the prediction parquet using PgDuckDB/DuckDB integration.
13. Java publishes rows into PostgreSQL table `lake_lake_customer_prediction`.
14. Axelor dashboards and AI Agent read customer prediction data from PostgreSQL.

## Component Responsibilities
- Axelor UI: Upload forms, dashboard views, AI Agent screen, menu navigation.
- Java controllers: Receive UI actions and publish callbacks.
- Java services: MinIO upload, Jenkins trigger, metadata handling, PostgreSQL publish step.
- MinIO: Stores raw CSV and generated parquet artifacts.
- Jenkins: Orchestrates dbt, ML prediction, and publish callback.
- dbt: Transforms raw customer data into staged lakehouse data.
- Python ML: Trains/runs customer churn risk prediction only.
- PostgreSQL: Stores published prediction rows used by dashboards and AI Agent.

## Important Files
- `modules/axelor-lake/src/main/java/com/axelor/lake/web/LakehouseController.java`
- `modules/axelor-lake/src/main/java/com/axelor/lake/service/LakehouseService.java`
- `modules/axelor-lake/src/main/java/com/axelor/lake/service/LakehouseServiceImpl.java`
- `modules/axelor-lake/src/main/java/com/axelor/lake/web/LakehousePublishEndpoint.java`
- `modules/axelor-lake/src/main/resources/views/LakehouseUpload.xml`
- `modules/axelor-lake/src/main/resources/views/LakeDashboard.xml`
- `Jenkinsfile`
- `python-ml/customer_risk.py`
- `ai-agent/app.py`

## Data Storage
- Raw CSV: MinIO `lake-raw`.
- Prediction parquet: MinIO analytics/artifact location.
- Published dashboard data: PostgreSQL `lake_lake_customer_prediction`.
- Lakehouse metadata/sync status: PostgreSQL `lake_lakehouse_table`.

## Dashboard Flow
1. Prediction pipeline finishes.
2. Publish endpoint loads prediction parquet into PostgreSQL.
3. Dashboard queries PostgreSQL prediction records.
4. Summary cards show active customers, high/medium/low risk counts, and churn percentages.
5. Prediction table shows customer-level prediction rows.

## AI Agent Flow
1. User asks a project question in Axelor AI Agent screen.
2. Axelor Java controller calls `ai-agent/ask.py`.
3. Python agent routes safe database prompts to PostgreSQL tools.
4. Project/code prompts use local project search tools.
5. Documentation prompts can write Markdown only under `ai-agent/generated-docs/`.
6. Unrelated questions are refused.

## Current Safe Agent Write Scope
The AI Agent can create generated Markdown documentation only in:

`ai-agent/generated-docs/`

It should not modify Java source, XML views, Jenkins files, credentials, or database data unless a separate permission is explicitly added.
