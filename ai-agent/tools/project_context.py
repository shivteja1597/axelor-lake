PROJECT_CONTEXT = """
Axelor Lakehouse project summary:

- Axelor Java module: modules/axelor-lake.
- Customer CSV upload starts from the Axelor UI form "Upload Customer Data".
- LakehouseController receives the upload action and calls LakehouseService.
- LakehouseServiceImpl uploads the raw CSV to MinIO bucket lake-raw under customer_profile/.
- Upload is replace-mode for the logical table, so re-uploading the same table clears old raw files first.
- Java triggers Jenkins job open-suite-lake-pipeline with TABLE_NAME=customer_profile.
- Jenkins runs dbt model stg_customer_profile and then python-ml/customer_risk.py.
- Python is only used for ML prediction. It writes customer_predictions.parquet to MinIO bucket lake-analytics.
- Jenkins then calls Axelor publish endpoint:
  /axelor-erp/ws/lake/customer-predictions/publish?tableName=customer_profile
- Java publish reads s3://lake-analytics/customer_predictions.parquet through PgDuckDB/DuckDB.
- Java inserts prediction rows into PostgreSQL table lake_lake_customer_prediction.
- Axelor dashboards read PostgreSQL, not MinIO, for customer prediction cards, segmentation, and table data.

Important project files:

- modules/axelor-lake/src/main/java/com/axelor/lake/web/LakehouseController.java
- modules/axelor-lake/src/main/java/com/axelor/lake/web/LakehousePublishEndpoint.java
- modules/axelor-lake/src/main/java/com/axelor/lake/service/LakehouseServiceImpl.java
- modules/axelor-lake/src/main/resources/views/LakeDashboard.xml
- modules/axelor-lake/src/main/resources/views/LakeCharts.xml
- modules/axelor-lake/src/main/resources/domains/CustomerPrediction.xml
- Jenkinsfile
- python-ml/customer_risk.py

PostgreSQL tables the agent may read:

- lake_lake_customer_prediction
- lake_lakehouse_table
- lake_lake_employee
- lake_lake_department
- lake_lake_role
- lake_lake_status
"""

