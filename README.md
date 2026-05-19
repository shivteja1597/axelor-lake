# Axelor Lake — Enterprise Lakehouse Analytics Platform

An end-to-end data lakehouse analytics platform built on top of [Axelor Open Suite](https://github.com/axelor/axelor-open-suite). It extends the Axelor ERP with a modern lakehouse architecture for employee profiling, customer risk prediction, and telecom churn analysis — all accessible directly from the Axelor UI.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Axelor ERP (Java 21)                        │
│   ┌──────────────┐   ┌────────────────┐   ┌─────────────────────┐  │
│   │ axelor-lake  │   │ axelor-employee│   │  Other AOS Modules  │  │
│   │  (Controller │   │  (Domain Model)│   │  (base, sale, etc.) │  │
│   │   + Service) │   │                │   │                     │  │
│   └──────┬───────┘   └────────────────┘   └─────────────────────┘  │
│          │                                                          │
└──────────┼──────────────────────────────────────────────────────────┘
           │  REST / HTTP
           ▼
┌──────────────────┐     ┌──────────────┐     ┌──────────────────────┐
│  Lakehouse API   │────▶│    MinIO      │     │      Nessie          │
│  (FastAPI/Python)│     │ (Object Store)│     │  (Iceberg Catalog)   │
│  + ML Models     │     │              │     │                      │
└────────┬─────────┘     └──────┬───────┘     └──────────────────────┘
         │                      │
         ▼                      ▼
┌──────────────────┐     ┌──────────────────┐
│    pg_duckdb     │     │  Jenkins CI/CD   │
│  (Analytics DB)  │     │  (dbt Pipeline)  │
└──────────────────┘     └──────────────────┘
                                │
                                ▼
                         ┌──────────────┐
                         │ dbt Lakehouse│
                         │ (Analytics   │
                         │  Models)     │
                         └──────────────┘
```

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **ERP** | Axelor Open Suite 9.0.3 | Core business application (Java 21, Gradle) |
| **Lakehouse API** | FastAPI (Python) | CSV ingestion, Iceberg table management, ML inference |
| **Object Storage** | MinIO | S3-compatible store for raw data, Parquet, and models |
| **Table Format** | Apache Iceberg | ACID-compliant table format with schema evolution |
| **Catalog** | Nessie 0.105 | Git-like version-controlled Iceberg catalog |
| **Analytics Engine** | pg_duckdb (PostgreSQL + DuckDB) | Query Parquet/Iceberg files via SQL |
| **Transformations** | dbt (DuckDB adapter) | Staging → Curated → Analytics data layers |
| **CI/CD** | Jenkins | Orchestrates dbt pipeline runs |
| **ML** | scikit-learn | Customer risk scoring & telecom churn prediction |
| **Database** | PostgreSQL 14 | Axelor application database |

---

## Project Structure

```
open-suite-webapp-master/
├── modules/
│   ├── axelor-lake/               # ★ Custom lakehouse module
│   │   ├── src/main/java/         # LakehouseController, LakehouseService
│   │   └── src/main/resources/
│   │       ├── domains/           # Axelor domain models (XML)
│   │       ├── views/             # UI forms, grids, dashboards, charts
│   │       └── lake_employee.csv  # Sample employee dataset
│   ├── axelor-employee/           # Custom employee module
│   ├── axelor-base/               # Axelor base module
│   ├── axelor-account/            # Accounting module
│   ├── axelor-sale/               # Sales module
│   └── ...                        # Other Axelor Open Suite modules
│
├── lakehouse-api/                 # FastAPI microservice
│   ├── main.py                    # REST endpoints (/upload, /query, /pipeline, etc.)
│   ├── ml/
│   │   ├── customer_risk.py       # Customer churn risk ML pipeline
│   │   └── telecom_churn.py       # Telecom churn prediction model
│   ├── requirements.txt
│   └── Dockerfile
│
├── dbt_lakehouse/                 # dbt project for data transformations
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/               # Raw → staging views
│       ├── curated/               # Staging → curated tables (dim_employee)
│       └── analytics/             # Pre-computed analytical reports
│           ├── employee_role_summary.sql
│           ├── employee_manager_summary.sql
│           ├── employee_department_salary_summary.sql
│           └── employee_salary_band.sql
│
├── jenkins/                       # Jenkins Docker configuration
│   └── Dockerfile
├── docker-compose.yml             # Full-stack orchestration
├── build.gradle                   # Root Gradle build (Java 21)
└── settings.gradle
```

---

## Key Features

### 📊 Employee Analytics Dashboard
- Upload employee CSV data into the lakehouse
- Data profiling with column-level statistics (distinct count, min, max, average)
- Filterable analysis by **department**, **role**, **status**, **age range**, and **salary range**
- Pre-computed dbt reports: role summary, department salary summary, manager summary, salary bands
- Paginated data grids with dynamic chart visualizations

### 🔮 Customer Risk Prediction
- Upload customer profile CSVs (contract, billing, property data)
- Automated ML pipeline: feature engineering → logistic regression → risk scoring
- Risk segments: **High Risk**, **Medium Risk**, **Low Risk**
- Customer segment-risk matrix visualization
- Summary KPIs: total customers, churn percentage by risk segment

### 📡 Telecom Churn Analysis
- Train churn prediction models on labeled telecom data
- Run predictions on new customer data
- Model stored in MinIO (`lake-models` bucket)

### 🏗️ Lakehouse Operations
- **Upload**: CSV → MinIO (raw) → Iceberg table (staging) → Jenkins pipeline → dbt → Parquet (analytics)
- **Query**: SQL via pg_duckdb querying Iceberg/Parquet files directly
- **Delete**: Full cleanup across catalog, staging, curated, analytics, and model storage
- **Purge**: Admin endpoint to wipe all lakehouse storage

---

## Prerequisites

- **Java 21** (for Axelor Open Suite)
- **Docker & Docker Compose** (for infrastructure services)
- **Python 3.13+** (for dbt and lakehouse-api, if running locally)
- **Git**

---

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/shivteja1597/axelor-lake.git
cd axelor-lake
```

### 2. Start Infrastructure Services

```bash
docker-compose up -d
```

This starts:
- **PostgreSQL 14** (Axelor DB) — `localhost:5432`
- **pg_duckdb** (Analytics) — `localhost:5433`
- **MinIO** (Object Storage) — API: `localhost:9000`, Console: `localhost:9001`
- **Nessie** (Iceberg Catalog) — `localhost:19120`
- **Jenkins** (CI/CD) — `localhost:8080`
- **Lakehouse API** (FastAPI) — `localhost:8000`

MinIO buckets (`lake-raw`, `lake-staging`, `lake-curated`, `lake-analytics`, `lake-models`) are auto-created on startup.

### 3. Build & Run Axelor

```bash
./gradlew build
./gradlew run
```

Axelor will be available at `http://localhost:8443` (default).

### 4. Configure Axelor

In `src/main/resources/axelor-config.properties`, ensure the database connection points to the PostgreSQL container:

```properties
db.default.url = jdbc:postgresql://localhost:5432/axelor
db.default.user = axelor
db.default.password = axelor
```

### 5. Set Up dbt (Optional — for local development)

```bash
cd dbt_lakehouse
python -m venv ../dbt_venv
source ../dbt_venv/bin/activate   # Linux/Mac
# ..\dbt_venv\Scripts\activate    # Windows
pip install dbt-duckdb
dbt run
```

---

## Lakehouse API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/upload` | Upload a CSV file to the lakehouse |
| `GET` | `/query/{table_name}` | Query an Iceberg table |
| `GET` | `/tables` | List all lakehouse tables |
| `GET` | `/metadata/{table_name}` | Get Iceberg metadata path |
| `DELETE` | `/tables/{table_name}` | Delete table and all artifacts |
| `POST` | `/pipeline/run` | Trigger Jenkins dbt pipeline |
| `POST` | `/admin/purge-storage` | Purge all MinIO buckets |

Full API docs available at `http://localhost:8000/docs` (Swagger UI).

---

## Data Flow

```
CSV Upload ──▶ MinIO (lake-raw) ──▶ Iceberg Table (lake-staging)
                                          │
                                    Jenkins Pipeline
                                          │
                                     dbt Transforms
                                          │
                              ┌───────────┼───────────┐
                              ▼           ▼           ▼
                         Curated     Analytics     ML Models
                        (Parquet)    (Parquet)    (.joblib)
                              │           │           │
                              └─────┬─────┘           │
                                    ▼                 │
                              pg_duckdb ◀─────────────┘
                                    │
                                    ▼
                            Axelor Dashboard
```

---

## Axelor Lake Module — Domain Models

| Entity | Description |
|---|---|
| `LakehouseTable` | Tracks uploaded Iceberg tables (name, metadata path, sync status) |
| `LakehouseUpload` | Wizard for uploading CSV files |
| `LakeEmployee` | Local cache of employee profiling data for fast filtering |
| `LakeDepartment` / `LakeRole` / `LakeStatus` | Lookup entities for filter dropdowns |

---

## dbt Models

| Layer | Model | Output |
|---|---|---|
| **Staging** | `stg_employee` | View over raw Iceberg data |
| **Curated** | `dim_employee` | Cleaned, typed employee dimension |
| **Analytics** | `employee_role_summary` | Employee count & avg salary by role |
| **Analytics** | `employee_manager_summary` | Summary by department × role |
| **Analytics** | `employee_department_salary_summary` | Salary stats by department |
| **Analytics** | `employee_salary_band` | Distribution across salary bands |

All analytics models are materialized as **Parquet files on MinIO** (`s3://lake-analytics/`).

---

## Environment Variables

### Lakehouse API

| Variable | Default | Description |
|---|---|---|
| `MINIO_ENDPOINT` | `http://minio:9000` | MinIO S3 endpoint |
| `MINIO_ACCESS_KEY` | `admin` | MinIO access key |
| `MINIO_SECRET_KEY` | `password123` | MinIO secret key |
| `CATALOG_URI` | `http://nessie:19120/iceberg/main` | Nessie Iceberg REST catalog |
| `PGDUCKDB_DSN` | `host=pgduckdb port=5432 ...` | pg_duckdb connection string |
| `JENKINS_URL` | `http://jenkins:8080` | Jenkins server URL |
| `JENKINS_JOB_URL` | — | Full URL to Jenkins pipeline job |
| `JENKINS_USER` | — | Jenkins username |
| `JENKINS_API_TOKEN` | — | Jenkins API token |

---

## License

This project is licensed under the [GNU Affero General Public License v3.0](LICENSE).

---

## Acknowledgements

- [Axelor Open Suite](https://github.com/axelor/axelor-open-suite) — ERP foundation
- [Apache Iceberg](https://iceberg.apache.org/) — Table format
- [Project Nessie](https://projectnessie.org/) — Catalog
- [DuckDB](https://duckdb.org/) / [pg_duckdb](https://github.com/duckdb/pg_duckdb) — Analytics engine
- [dbt](https://www.getdbt.com/) — Data transformations
- [MinIO](https://min.io/) — Object storage
