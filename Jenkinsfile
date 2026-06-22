pipeline {
  agent any

  environment {
    WORKSPACE_DIR = '/var/jenkins_home/project'
    DBT_DIR = "${WORKSPACE_DIR}/dbt_lakehouse"
    AXELOR_PUBLISH_BASE_URL = 'http://host.docker.internal:8060/axelor-erp'
    AXELOR_CALLBACK_USER = 'admin'
    AXELOR_CALLBACK_PASSWORD = 'admin'
    MINIO_ENDPOINT = 'http://minio:9000'
    MINIO_ENDPOINT_HOSTPORT = 'minio:9000'
    MINIO_ACCESS_KEY = 'admin'
    MINIO_SECRET_KEY = 'password123'
    MINIO_REGION = 'us-east-1'
    ICEBERG_REST_URI = 'http://nessie:19120/iceberg'
    ICEBERG_WAREHOUSE = 'warehouse'
    ICEBERG_CATALOG_ALIAS = 'iceberg_lake'
  }

  parameters {
    string(name: 'TABLE_NAME', defaultValue: '', description: 'Lake dataset name')
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
            env.LAKE_TABLE_NAME = (params.TABLE_NAME ?: env.TABLE_NAME ?: env.table_name ?: '').trim()
            if (!env.LAKE_TABLE_NAME) {
              error('TABLE_NAME is required for the lakehouse pipeline.')
            }
            env.LAKE_DATASET_KIND = env.LAKE_TABLE_NAME == 'customer_profile' ? 'customer_profile' : 'employee_profile'
            echo "Detected lakehouse dataset kind: ${env.LAKE_DATASET_KIND}"
            echo "Lake table name: ${env.LAKE_TABLE_NAME}"
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
            pip install --no-cache-dir dbt-duckdb==1.10.1 duckdb==1.5.3

            # Navigate to dbt project explicitly using the absolute directory
            cd dbt_lakehouse
            export EMPLOYEE_DATA_GLOB="s3://lake-raw/${LAKE_TABLE_NAME}/*.csv"

            # Run dbt pipelines
            dbt deps
            dbt run --profiles-dir . --select stg_employee dim_employee
            dbt run --profiles-dir . --select employee_salary_band employee_role_summary employee_manager_summary
            dbt run --profiles-dir . --select employee_department_salary_summary
            dbt test --profiles-dir . --select dim_employee+
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
            pip install --no-cache-dir dbt-duckdb==1.10.1 duckdb==1.5.3 pandas scikit-learn joblib boto3
            export CUSTOMER_PROFILE_DATA_GLOB="s3://lake-raw/${LAKE_TABLE_NAME}/*.csv"
            cd dbt_lakehouse
            dbt deps
            dbt run --profiles-dir . --select stg_customer_profile customer_profile_features
            cd ..
            python python-ml/customer_risk.py
            cd dbt_lakehouse
            dbt run --profiles-dir . --select customer_predictions customer_segments
            cd ..

            echo "Publishing customer predictions to Axelor PostgreSQL..."
            echo "Axelor publish URL: ${AXELOR_PUBLISH_BASE_URL}/ws/lake/customer-predictions/publish?tableName=${LAKE_TABLE_NAME}"
            response_file="$(mktemp)"
            http_code="$(curl --show-error --silent --output "${response_file}" --write-out "%{http_code}" \
              --user "${AXELOR_CALLBACK_USER}:${AXELOR_CALLBACK_PASSWORD}" \
              -X POST "${AXELOR_PUBLISH_BASE_URL}/ws/lake/customer-predictions/publish?tableName=${LAKE_TABLE_NAME}")"
            cat "${response_file}"
            if [ "${http_code}" -lt 200 ] || [ "${http_code}" -ge 300 ]; then
              echo "Axelor publish failed with HTTP ${http_code}"
              exit 1
            fi
          '''
        }
      }
    }
  }
}
