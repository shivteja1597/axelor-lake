# Axelor Lake

Axelor Lake extends Axelor Open Suite with lakehouse-style employee analytics and customer risk prediction.

## Overview

- Java/Axelor handles upload, MinIO storage, querying, cleanup, UI actions, and dashboards.
- Jenkins and dbt build curated and analytics outputs from raw uploaded data.
- Python is used only for the customer risk ML workflow.
- Dashboards read raw CSV or parquet outputs from MinIO through PgDuckDB.

## Main Components

- `modules/axelor-lake`
  Java module with controllers, services, views, and dashboard integration.
- `python-ml`
  Python customer risk workflow used by Jenkins.
- `dbt_lakehouse`
  Transformation project for staging, curated, and analytics datasets.
- `sample-data`
  Sample input files for supported flows.

## Current Features

### Employee Analytics

- Upload employee CSV data
- Build local profiling cache
- Filter by department, role, status, age, and salary
- Show employee dashboards backed by MinIO and PgDuckDB

### Customer Risk Prediction

- Upload customer profile CSV data
- Trigger Jenkins/dbt processing
- Run customer risk scoring in Python
- Show prediction table, KPI summary, and segmentation dashboards

## Data Flow

1. User uploads CSV from Axelor.
2. Java stores the raw CSV in MinIO.
3. Jenkins/dbt prepares curated and analytics outputs.
4. Python runs customer risk scoring and writes prediction artifacts to MinIO.
5. Axelor dashboards query MinIO-backed data through PgDuckDB.

## Environment Notes

Important services used by the project:

- PostgreSQL for Axelor
- PgDuckDB for analytics queries
- MinIO for raw, curated, analytics, and model artifacts
- Jenkins for orchestration
- Python for the customer risk workflow

## Repository Notes

- The remaining ML flow is customer risk only.
