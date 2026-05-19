pipeline {
  agent any

  environment {
    WORKSPACE_DIR = '/var/jenkins_home/project'
    DBT_DIR = "${WORKSPACE_DIR}/dbt_lakehouse"
  }

  stages {
    stage('Check Workspace') {
      steps {
        dir("${WORKSPACE_DIR}") {
          sh 'pwd'
          sh 'ls'
        }
      }
    }

    stage('Detect Dataset Type') {
      steps {
        dir("${WORKSPACE_DIR}") {
          script {
            env.LAKE_DATASET_KIND = sh(
              script: '''
                python3 -c "import json, urllib.request; data = json.load(urllib.request.urlopen('http://lakehouse-api:8000/datasets/latest')); print(data.get('dataset_kind', 'unknown'))"
              ''',
              returnStdout: true
            ).trim()
            env.LAKE_METADATA_PATH = sh(
              script: '''
                python3 -c "import json, urllib.request; data = json.load(urllib.request.urlopen('http://lakehouse-api:8000/datasets/latest')); print(data.get('metadata_path', ''))"
              ''',
              returnStdout: true
            ).trim()
            echo "Detected lakehouse dataset kind: ${env.LAKE_DATASET_KIND}"
            echo "Latest metadata path: ${env.LAKE_METADATA_PATH}"
          }
        }
      }
    }

    stage('Run dbt Transformation') {
      when {
        expression { env.LAKE_DATASET_KIND == 'employee_profile' }
      }
      steps {
        dir("${WORKSPACE_DIR}") {
          sh '''
            # Use virtualenv natively in Jenkins for dbt only.
            python3 -m venv dbt_venv
            . dbt_venv/bin/activate
            
            # Refresh packaging tooling before installing dbt.
            pip install --no-cache-dir --upgrade pip setuptools wheel

            # Install dbt dependencies securely in workspace
            pip install --no-cache-dir dbt-duckdb

            # Navigate to dbt project explicitly using the absolute directory
            cd dbt_lakehouse

            # Run dbt pipelines
            dbt deps
            dbt run --profiles-dir . --select stg_employee+
            dbt test --profiles-dir . --select stg_employee+
          '''
        }
      }
    }

    stage('Run Customer Risk Workflow') {
      when {
        expression { env.LAKE_DATASET_KIND == 'customer_profile' }
      }
      steps {
        dir("${WORKSPACE_DIR}") {
          sh '''
            python3 -m venv dbt_venv
            . dbt_venv/bin/activate
            pip install --no-cache-dir --upgrade pip setuptools wheel
            pip install --no-cache-dir dbt-duckdb pandas scikit-learn joblib boto3
            export CUSTOMER_PROFILE_DATA_GLOB="$(python3 - <<'PY'
from urllib.parse import urlparse
import os

metadata_path = os.environ.get('LAKE_METADATA_PATH', '').strip()
if not metadata_path:
    raise SystemExit('LAKE_METADATA_PATH is not set for customer workflow')

parsed = urlparse(metadata_path)
path = parsed.path.lstrip('/')
prefix = path.rsplit('/metadata/', 1)[0]
print(f"s3://{parsed.netloc}/{prefix}/data/*.parquet")
PY
)"
            cd dbt_lakehouse
            dbt deps
            dbt run --profiles-dir . --select stg_customer_profile+
            cd ..
            python lakehouse-api/ml/customer_risk.py
          '''
        }
      }
    }
  }
}
