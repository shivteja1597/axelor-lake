/*
 * Axelor Business Solutions
 *
 * Copyright (C) 2005-2026 Axelor (<http://axelor.com>).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package com.axelor.lake.service;

import com.axelor.app.AppSettings;
import com.axelor.concurrent.ContextAware;
import com.axelor.db.JPA;
import com.axelor.db.tenants.TenantResolver;
import com.axelor.lake.db.LakeCustomerPrediction;
import com.axelor.lake.db.LakeDepartment;
import com.axelor.lake.db.LakeEmployee;
import com.axelor.lake.db.LakeRole;
import com.axelor.lake.db.LakeStatus;
import com.axelor.lake.db.LakehouseTable;
import com.axelor.lake.db.repo.LakehouseTableRepository;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.persist.Transactional;
import io.minio.BucketExistsArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.RemoveObjectArgs;
import io.minio.Result;
import io.minio.UploadObjectArgs;
import io.minio.messages.Item;
import jakarta.persistence.EntityManager;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;
import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class LakehouseServiceImpl implements LakehouseService {

  private static final Logger LOG = LoggerFactory.getLogger(LakehouseServiceImpl.class);
  private static final String SYNC_STATUS_READY = "Ready";
  private static final String SYNC_STATUS_PENDING = "Pending";
  private static final String SYNC_STATUS_COMPLETED = "Completed";
  private static final String SYNC_STATUS_FAILED = "Failed";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final int BUFFER_SIZE = 8192;
  private static final int EMPLOYEE_SYNC_BATCH_SIZE = 2000;
  private static final int PREDICTION_SYNC_BATCH_SIZE = 1000;
  private static final Pattern TABLE_NAME_PATTERN = Pattern.compile("^[A-Za-z0-9_]+$");
  private static final String ICEBERG_TABLE_PREFIX = "iceberg-table:";

  private final LakehouseTableRepository lakehouseTableRepository;
  private final HttpClient httpClient;
  private final ExecutorService employeeSyncExecutor;

  @Inject
  public LakehouseServiceImpl(LakehouseTableRepository lakehouseTableRepository) {
    this.lakehouseTableRepository = lakehouseTableRepository;
    this.httpClient =
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();
    this.employeeSyncExecutor =
        Executors.newSingleThreadExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "lake-employee-sync-executor");
              thread.setDaemon(true);
              return thread;
            });
  }

  @Override
  @Transactional(rollbackOn = {Exception.class})
  public LakehouseTable uploadToLakehouse(File csv, String tableName)
      throws IOException, InterruptedException, SQLException {
    return uploadToLakehouse(csv, tableName, true);
  }

  @Override
  @Transactional(rollbackOn = {Exception.class})
  public LakehouseTable uploadToLakehouse(File csv, String tableName, boolean triggerPipeline)
      throws IOException, InterruptedException, SQLException {
    final String normalizedTableName = validateCsv(csv, tableName);
    // Replace-mode upload: keep only the latest raw dataset for each logical table.
    deletePrefix(getMinioRawBucket(), normalizedTableName + "/");
    final String rawObjectKey = buildRawObjectKey(normalizedTableName, csv.getName());
    uploadRawCsv(csv.toPath(), rawObjectKey);
    final String metadataPath = getStagingIcebergTableReference(normalizedTableName);
    final long uploadedRowCount = countCsvRows(csv.toPath());

    LakehouseTable record =
        lakehouseTableRepository
            .all()
            .filter("self.tableName = ?1", normalizedTableName)
            .fetchOne();
    if (record == null) {
      record = new LakehouseTable();
      record.setTableName(normalizedTableName);
    }
    record.setMetadataPath(metadataPath);
    record.setUploadedAt(LocalDateTime.now());
    record.setUploadedRowCount(uploadedRowCount);
    record.setRowCount(uploadedRowCount);

    if (triggerPipeline) {
      triggerPipeline(normalizedTableName);
    }

    if (triggerPipeline) {
      record.setSyncStatus(SYNC_STATUS_PENDING);
      record.setLastSyncAt(null);
      record.setSyncMessage("dbt lakehouse pipeline is running in Jenkins.");
    } else {
      record.setSyncStatus(SYNC_STATUS_READY);
      record.setLastSyncAt(LocalDateTime.now());
      record.setSyncMessage(null);
    }
    record = lakehouseTableRepository.save(record);

    return record;
  }

  public List<Map<String, Object>> queryFromLakehouse(String tableName)
      throws IOException, InterruptedException, SQLException {
    return queryFromLakehouse(tableName, 0);
  }

  @Override
  public List<Map<String, Object>> queryFromLakehouse(String tableName, int limit)
      throws IOException, InterruptedException, SQLException {
    final String metadataPath = getLatestMetadataPath(tableName);
    return queryFromLakehouseByMetadataPath(metadataPath, limit);
  }

  @Override
  public List<Map<String, Object>> queryFromLakehouseByMetadataPath(String metadataPath, int limit)
      throws SQLException {
    try (Connection connection =
            DriverManager.getConnection(
                getPgDuckDbJdbcUrl(), getPgDuckDbUsername(), getPgDuckDbPassword());
        Statement statement = createLakehouseStatement(connection);
        ResultSet resultSet = statement.executeQuery(buildLakehouseQuery(metadataPath, limit))) {
      return mapRows(resultSet);
    }
  }

  @Override
  public void prepareProfilingData(String tableName, String metadataPath) throws SQLException {
    if (!isProfileCompatibleTable(metadataPath)) {
      throw new IllegalStateException(
          "Data profiling requires employeeId, name, department, age, salary, and status columns.");
    }
    syncEmployees(tableName, metadataPath);
  }

  @Override
  public void prepareProfilingDataAsync(String tableName, String metadataPath) throws SQLException {
    if (!isProfileCompatibleTable(metadataPath)) {
      throw new IllegalStateException(
          "Data profiling requires employeeId, name, department, age, salary, and status columns.");
    }
    publishSyncProgress(tableName, "Profile data sync is running in background.");
    scheduleEmployeesSyncIfNeeded(tableName, metadataPath, true);
  }

  @Override
  public int publishCustomerPredictions(String tableName) throws SQLException {
    final String normalizedTableName = validateTableName(tableName);
    if (!isCustomerPredictionTable(normalizedTableName)) {
      throw new IllegalArgumentException(
          "Customer prediction publish is only supported for customer_profile.");
    }
    try {
      return syncCustomerPredictions(normalizedTableName);
    } catch (Exception e) {
      JPA.runInTransaction(
          () ->
              updateSyncStatus(
                  normalizedTableName,
                  SYNC_STATUS_FAILED,
                  abbreviateMessage(e.getMessage(), 1000),
                  LocalDateTime.now()));
      throw e;
    }
  }

  @Override
  public String getLatestMetadataPath(String tableName) throws IOException, InterruptedException {
    final String normalizedTableName = validateTableName(tableName);
    LakehouseTable table =
        lakehouseTableRepository
            .all()
            .filter("self.tableName = ?1", normalizedTableName)
            .fetchOne();
    if (table == null || table.getMetadataPath() == null || table.getMetadataPath().isBlank()) {
      throw new IllegalStateException(
          "No lakehouse metadata found for table: " + normalizedTableName);
    }
    return table.getMetadataPath();
  }

  @Override
  public void deleteTable(String tableName) throws IOException, InterruptedException {
    final String normalizedTableName = validateTableName(tableName);
    deletePrefix(getMinioRawBucket(), normalizedTableName + "/");

    for (String objectPath : getDerivedArtifactPaths(normalizedTableName)) {
      deleteObjectAtPathIfPresent(objectPath);
    }
    deletePublishedPredictionData(normalizedTableName);
  }

  private String uploadRawCsv(Path csvPath, String objectKey) throws IOException {
    ensureBucketExists(getMinioRawBucket());

    try {
      minioClient()
          .uploadObject(
              UploadObjectArgs.builder()
                  .bucket(getMinioRawBucket())
                  .object(objectKey)
                  .filename(csvPath.toString())
                  .contentType("text/csv")
                  .build());
    } catch (Exception e) {
      throw new IOException("Unable to upload CSV to MinIO.", e);
    }

    return "s3://" + getMinioRawBucket() + "/" + objectKey;
  }

  private long countCsvRows(Path csvPath) throws IOException {
    try (BufferedReader reader = Files.newBufferedReader(csvPath, StandardCharsets.UTF_8)) {
      long count = -1L;
      while (reader.readLine() != null) {
        count++;
      }
      return Math.max(0L, count);
    }
  }

  private String buildRawObjectKey(String tableName, String fileName) {
    String sanitizedName = fileName == null || fileName.isBlank() ? tableName + ".csv" : fileName;
    return tableName + "/" + System.currentTimeMillis() + "_" + sanitizedName;
  }

  private MinioClient minioClient() {
    return MinioClient.builder()
        .endpoint(getMinioEndpoint())
        .credentials(getMinioAccessKey(), getMinioSecretKey())
        .build();
  }

  private void ensureBucketExists(String bucketName) throws IOException {
    try {
      MinioClient minioClient = minioClient();
      boolean exists =
          minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
      if (!exists) {
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
      }
    } catch (Exception e) {
      throw new IOException("Unable to ensure MinIO bucket exists: " + bucketName, e);
    }
  }

  private void deleteObjectAtPathIfPresent(String objectPath) throws IOException {
    if (objectPath == null || objectPath.isBlank() || !objectPath.startsWith("s3://")) {
      return;
    }

    String withoutScheme = objectPath.substring("s3://".length());
    int slashIndex = withoutScheme.indexOf('/');
    if (slashIndex < 0) {
      return;
    }

    deleteObject(withoutScheme.substring(0, slashIndex), withoutScheme.substring(slashIndex + 1));
  }

  private void deleteObject(String bucketName, String objectName) throws IOException {
    if (bucketName == null || bucketName.isBlank() || objectName == null || objectName.isBlank()) {
      return;
    }

    try {
      minioClient()
          .removeObject(RemoveObjectArgs.builder().bucket(bucketName).object(objectName).build());
    } catch (Exception e) {
      throw new IOException(
          "Unable to delete MinIO object: s3://" + bucketName + "/" + objectName, e);
    }
  }

  private void deletePrefix(String bucketName, String prefix) throws IOException {
    if (bucketName == null || bucketName.isBlank() || prefix == null || prefix.isBlank()) {
      return;
    }

    try {
      for (Result<Item> result :
          minioClient()
              .listObjects(
                  ListObjectsArgs.builder()
                      .bucket(bucketName)
                      .prefix(prefix)
                      .recursive(true)
                      .build())) {
        Item item = result.get();
        if (item != null && item.objectName() != null && !item.objectName().isBlank()) {
          deleteObject(bucketName, item.objectName());
        }
      }
    } catch (Exception e) {
      throw new IOException("Unable to delete MinIO objects for prefix: " + prefix, e);
    }
  }

  private List<String> getDerivedArtifactPaths(String tableName) {
    if ("customer_profile".equalsIgnoreCase(tableName)) {
      return List.of(
          "s3://" + getMinioCuratedBucket() + "/customer_profile_features.parquet",
          "s3://" + getMinioAnalyticsBucket() + "/customer_predictions.parquet",
          "s3://" + getMinioAnalyticsBucket() + "/customer_segments.parquet",
          "s3://" + getMinioModelsBucket() + "/customer-risk/logistic_regression.joblib");
    }
    return List.of(
        "s3://" + getMinioCuratedBucket() + "/dim_employee.parquet",
        "s3://" + getMinioAnalyticsBucket() + "/employee_role_summary.parquet",
        "s3://" + getMinioAnalyticsBucket() + "/employee_manager_summary.parquet",
        "s3://" + getMinioAnalyticsBucket() + "/employee_department_salary_summary.parquet",
        "s3://" + getMinioAnalyticsBucket() + "/employee_salary_band.parquet");
  }

  private void scheduleEmployeesSyncIfNeeded(
      String tableName, String metadataPath, boolean profileCompatible) {
    if (!profileCompatible) {
      return;
    }

    final String currentTenantId = TenantResolver.currentTenantIdentifier();
    employeeSyncExecutor.submit(
        ContextAware.of()
            .withTransaction(false)
            .withTenantId(currentTenantId)
            .build(
                () -> {
                  try {
                    syncEmployees(tableName, metadataPath);
                  } catch (Exception e) {
                    LOG.error("Profile sync failed for table {}", tableName, e);
                    JPA.runInTransaction(
                        () ->
                            updateSyncStatus(
                                tableName,
                                SYNC_STATUS_FAILED,
                                abbreviateMessage(e.getMessage(), 1000),
                                LocalDateTime.now()));
                  }
                  return null;
                }));
  }

  private void syncEmployees(String tableName, String metadataPath) throws SQLException {
    publishSyncProgress(tableName, "Reading profile data from lakehouse...");

    final List<LakeEmployee> employees = new ArrayList<>();
    int scannedRowCount = 0;

    try (Connection connection =
            DriverManager.getConnection(
                getPgDuckDbJdbcUrl(), getPgDuckDbUsername(), getPgDuckDbPassword());
        Statement statement = createLakehouseStatement(connection);
        ResultSet resultSet = statement.executeQuery(buildLakehouseQuery(metadataPath, 0))) {
      final Map<String, Integer> columnIndexes = resolveColumnIndexes(resultSet.getMetaData());
      while (resultSet.next()) {
        scannedRowCount++;

        final LakeEmployee employee = new LakeEmployee();
        employee.setEmployeeId(
            getRequiredRowValue(resultSet, columnIndexes, "employeeId", "employee_id", "id"));
        employee.setName(
            getRequiredRowValue(resultSet, columnIndexes, "name", "employeeName", "employee_name"));
        employee.setDepartment(getRequiredRowValue(resultSet, columnIndexes, "department", "dept"));
        employee.setAge(getRequiredIntegerValue(resultSet, columnIndexes, "age", "employeeAge"));
        employee.setSalary(
            getRequiredDecimalValue(resultSet, columnIndexes, "salary", "employeeSalary"));
        employee.setStatus(
            getRequiredRowValue(resultSet, columnIndexes, "status", "employeeStatus"));
        employee.setRole(getOptionalRowValue(resultSet, columnIndexes, "role", "employeeRole"));
        employees.add(employee);

        if (scannedRowCount % EMPLOYEE_SYNC_BATCH_SIZE == 0) {
          publishSyncProgress(
              tableName,
              String.format(
                  Locale.ROOT,
                  "Reading profile data from lakehouse... %,d rows scanned.",
                  scannedRowCount));
        }
      }
    }

    publishSyncProgress(
        tableName,
        String.format(
            Locale.ROOT,
            "Preparing local employee table... %,d rows scanned and %,d rows ready to load.",
            scannedRowCount,
            employees.size()));

    final int finalScannedRowCount = scannedRowCount;
    JPA.runInTransaction(
        () -> {
          bulkReplaceEmployees(employees);
          bulkRefreshLookupValues(employees);

          LakehouseTable table =
              lakehouseTableRepository.all().filter("self.tableName = ?1", tableName).fetchOne();
          if (table != null) {
            table.setRowCount(Long.valueOf(finalScannedRowCount));
            lakehouseTableRepository.save(table);
          }

          updateSyncStatus(
              tableName,
              SYNC_STATUS_COMPLETED,
              String.format(
                  Locale.ROOT,
                  "Profile sync completed. Scanned %,d rows and loaded %,d rows into profiling data.",
                  finalScannedRowCount,
                  employees.size()),
              LocalDateTime.now());
        });
  }

  private int syncCustomerPredictions(String tableName) throws SQLException {
    return syncCustomerPredictionsFromIceberg(
        tableName, getCustomerPredictionsIcebergTableReference());
  }

  private int syncCustomerPredictionsFromIceberg(String tableName, String metadataPath)
      throws SQLException {
    publishSyncProgress(tableName, "Publishing customer predictions from Iceberg to PostgreSQL...");

    final List<LakeCustomerPrediction> predictions = new ArrayList<>();
    int scannedRowCount = 0;

    try (Connection connection =
            DriverManager.getConnection(
                getPgDuckDbJdbcUrl(), getPgDuckDbUsername(), getPgDuckDbPassword());
        Statement statement = createLakehouseStatement(connection);
        ResultSet resultSet = statement.executeQuery(buildLakehouseQuery(metadataPath, 0))) {
      final Map<String, Integer> columnIndexes = resolveColumnIndexes(resultSet.getMetaData());
      while (resultSet.next()) {
        scannedRowCount++;

        LakeCustomerPrediction prediction = new LakeCustomerPrediction();
        prediction.setAccountId(getRequiredRowValue(resultSet, columnIndexes, "account_id"));
        prediction.setSiteId(getOptionalRowValue(resultSet, columnIndexes, "site_id"));
        prediction.setCustomerName(getRequiredRowValue(resultSet, columnIndexes, "customer_name"));
        prediction.setState(getOptionalRowValue(resultSet, columnIndexes, "state"));
        prediction.setContractStatus(
            getOptionalRowValue(resultSet, columnIndexes, "contract_status"));
        prediction.setPlanType(getOptionalRowValue(resultSet, columnIndexes, "plan_type"));
        prediction.setCurrentRmr(getOptionalDecimalValue(resultSet, columnIndexes, "current_rmr"));
        prediction.setCustomerSegmentBucket(
            getOptionalRowValue(resultSet, columnIndexes, "customer_segment_bucket"));
        prediction.setChurnRiskPercentage(
            getOptionalDecimalValue(resultSet, columnIndexes, "churn_risk_percentage"));
        prediction.setRiskSegment(getOptionalRowValue(resultSet, columnIndexes, "risk_segment"));
        prediction.setBaseRisk(getOptionalDecimalValue(resultSet, columnIndexes, "base_risk"));
        prediction.setPositiveRiskDrivers(getOptionalRowValue(resultSet, columnIndexes, "positive_risk_drivers"));
        prediction.setNegativeRiskDrivers(getOptionalRowValue(resultSet, columnIndexes, "negative_risk_drivers"));
        predictions.add(prediction);

        if (scannedRowCount % PREDICTION_SYNC_BATCH_SIZE == 0) {
          publishSyncProgress(
              tableName,
              String.format(
                  Locale.ROOT,
                  "Publishing customer predictions... %,d rows scanned.",
                  scannedRowCount));
        }
      }
    }

    final int finalScannedRowCount = scannedRowCount;
    JPA.runInTransaction(
        () -> {
          bulkReplaceCustomerPredictions(predictions);
          updateSyncStatus(
              tableName,
              SYNC_STATUS_COMPLETED,
              String.format(
                  Locale.ROOT,
                  "Customer predictions published to PostgreSQL. %,d rows loaded.",
                  finalScannedRowCount),
              LocalDateTime.now());
        });
    return finalScannedRowCount;
  }

  private void publishSyncProgress(String tableName, String syncMessage) {
    JPA.runInTransaction(() -> updateSyncStatus(tableName, SYNC_STATUS_PENDING, syncMessage, null));
  }

  private void refreshLookupValues(List<LakeEmployee> employees) {
    final EntityManager entityManager = JPA.em();
    entityManager.flush();
    entityManager.clear();
    entityManager.createQuery("DELETE FROM LakeDepartment").executeUpdate();
    entityManager.createQuery("DELETE FROM LakeRole").executeUpdate();
    entityManager.createQuery("DELETE FROM LakeStatus").executeUpdate();
    entityManager.flush();
    entityManager.clear();

    TreeSet<String> departments = new TreeSet<>();
    TreeSet<String> roles = new TreeSet<>();
    TreeSet<String> statuses = new TreeSet<>();
    for (LakeEmployee employee : employees) {
      addLookupValue(departments, employee.getDepartment());
      addLookupValue(roles, employee.getRole());
      addLookupValue(statuses, employee.getStatus());
    }

    persistLookupValues(entityManager, departments, LakeDepartment.class);
    persistLookupValues(entityManager, roles, LakeRole.class);
    persistLookupValues(entityManager, statuses, LakeStatus.class);
    entityManager.flush();
  }

  private void bulkReplaceEmployees(List<LakeEmployee> employees) {
    JPA.em()
        .unwrap(Session.class)
        .doWork(
            connection -> {
              truncateTable(connection, "lake_lake_employee", "lake_lake_employee_seq");
              if (employees.isEmpty()) {
                return;
              }

              try (PreparedStatement statement =
                  connection.prepareStatement(
                      "INSERT INTO lake_lake_employee "
                          + "(id, version, age, department, employee_id, name, role, salary, status) "
                          + "VALUES (nextval('lake_lake_employee_seq'), 0, ?, ?, ?, ?, ?, ?, ?)")) {
                int batchSize = 0;
                for (LakeEmployee employee : employees) {
                  statement.setInt(1, employee.getAge());
                  statement.setString(2, employee.getDepartment());
                  statement.setString(3, employee.getEmployeeId());
                  statement.setString(4, employee.getName());
                  statement.setString(5, employee.getRole());
                  statement.setBigDecimal(6, employee.getSalary());
                  statement.setString(7, employee.getStatus());
                  statement.addBatch();
                  batchSize++;

                  if (batchSize % EMPLOYEE_SYNC_BATCH_SIZE == 0) {
                    statement.executeBatch();
                  }
                }
                statement.executeBatch();
              }
            });
  }

  private void bulkReplaceCustomerPredictions(List<LakeCustomerPrediction> predictions) {
    JPA.em()
        .unwrap(Session.class)
        .doWork(
            connection -> {
              truncateTable(
                  connection, "lake_lake_customer_prediction", "lake_lake_customer_prediction_seq");
              if (predictions.isEmpty()) {
                return;
              }

              try (PreparedStatement statement =
                  connection.prepareStatement(
                      "INSERT INTO lake_lake_customer_prediction "
                          + "(id, version, account_id, site_id, customer_name, state, contract_status, "
                          + "plan_type, current_rmr, customer_segment_bucket, churn_risk_percentage, risk_segment, base_risk, positive_risk_drivers, negative_risk_drivers) "
                          + "VALUES (nextval('lake_lake_customer_prediction_seq'), 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
                int batchSize = 0;
                for (LakeCustomerPrediction prediction : predictions) {
                  statement.setString(1, prediction.getAccountId());
                  statement.setString(2, prediction.getSiteId());
                  statement.setString(3, prediction.getCustomerName());
                  statement.setString(4, prediction.getState());
                  statement.setString(5, prediction.getContractStatus());
                  statement.setString(6, prediction.getPlanType());
                  statement.setBigDecimal(7, prediction.getCurrentRmr());
                  statement.setString(8, prediction.getCustomerSegmentBucket());
                  statement.setBigDecimal(9, prediction.getChurnRiskPercentage());
                  statement.setString(10, prediction.getRiskSegment());
                  statement.setBigDecimal(11, prediction.getBaseRisk());
                  statement.setString(12, prediction.getPositiveRiskDrivers());
                  statement.setString(13, prediction.getNegativeRiskDrivers());
                  statement.addBatch();
                  batchSize++;

                  if (batchSize % PREDICTION_SYNC_BATCH_SIZE == 0) {
                    statement.executeBatch();
                  }
                }
                statement.executeBatch();
              }
            });
  }

  private void bulkRefreshLookupValues(List<LakeEmployee> employees) {
    TreeSet<String> departments = new TreeSet<>();
    TreeSet<String> roles = new TreeSet<>();
    TreeSet<String> statuses = new TreeSet<>();
    for (LakeEmployee employee : employees) {
      addLookupValue(departments, employee.getDepartment());
      addLookupValue(roles, employee.getRole());
      addLookupValue(statuses, employee.getStatus());
    }

    JPA.em()
        .unwrap(Session.class)
        .doWork(
            connection -> {
              truncateTable(connection, "lake_lake_department", "lake_lake_department_seq");
              truncateTable(connection, "lake_lake_role", "lake_lake_role_seq");
              truncateTable(connection, "lake_lake_status", "lake_lake_status_seq");
              insertLookupValues(
                  connection, "lake_lake_department", "lake_lake_department_seq", departments);
              insertLookupValues(connection, "lake_lake_role", "lake_lake_role_seq", roles);
              insertLookupValues(connection, "lake_lake_status", "lake_lake_status_seq", statuses);
            });
  }

  private void truncateTable(Connection connection, String tableName, String sequenceName)
      throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute("TRUNCATE TABLE " + tableName);
      statement.execute("ALTER SEQUENCE " + sequenceName + " RESTART WITH 1");
    }
  }

  private void insertLookupValues(
      Connection connection, String tableName, String sequenceName, TreeSet<String> values)
      throws SQLException {
    if (values.isEmpty()) {
      return;
    }

    try (PreparedStatement statement =
        connection.prepareStatement(
            "INSERT INTO "
                + tableName
                + " (id, version, name) VALUES (nextval('"
                + sequenceName
                + "'), 0, ?)")) {
      for (String value : values) {
        statement.setString(1, value);
        statement.addBatch();
      }
      statement.executeBatch();
    }
  }

  private void addLookupValue(TreeSet<String> values, String value) {
    if (value != null && !value.isBlank()) {
      values.add(value.trim());
    }
  }

  private <T> void persistLookupValues(
      EntityManager entityManager, TreeSet<String> values, Class<T> entityClass) {
    for (String value : values) {
      if (entityClass.equals(LakeDepartment.class)) {
        LakeDepartment entity = new LakeDepartment();
        entity.setName(value);
        entityManager.persist(entity);
      } else if (entityClass.equals(LakeRole.class)) {
        LakeRole entity = new LakeRole();
        entity.setName(value);
        entityManager.persist(entity);
      } else if (entityClass.equals(LakeStatus.class)) {
        LakeStatus entity = new LakeStatus();
        entity.setName(value);
        entityManager.persist(entity);
      }
    }
  }

  private void persistEmployeeBatch(List<LakeEmployee> employees) {
    final EntityManager entityManager = JPA.em();
    for (LakeEmployee employee : employees) {
      entityManager.persist(employee);
    }
    entityManager.flush();
    entityManager.clear();
  }

  private void persistEmployeeBatches(List<LakeEmployee> employees) {
    for (int start = 0; start < employees.size(); start += EMPLOYEE_SYNC_BATCH_SIZE) {
      final int end = Math.min(start + EMPLOYEE_SYNC_BATCH_SIZE, employees.size());
      persistEmployeeBatch(employees.subList(start, end));
    }
  }

  private void updateSyncStatus(
      String tableName, String syncStatus, String syncMessage, LocalDateTime lastSyncAt) {
    JPA.em()
        .createQuery(
            "UPDATE LakehouseTable self "
                + "SET self.syncStatus = :syncStatus, "
                + "self.syncMessage = :syncMessage, "
                + "self.lastSyncAt = :lastSyncAt "
                + "WHERE self.tableName = :tableName")
        .setParameter("syncStatus", syncStatus)
        .setParameter("syncMessage", syncMessage)
        .setParameter("lastSyncAt", lastSyncAt)
        .setParameter("tableName", tableName)
        .executeUpdate();
  }

  private boolean isCustomerPredictionTable(String tableName) {
    return "customer_profile".equalsIgnoreCase(tableName);
  }

  private void deletePublishedPredictionData(String tableName) {
    if (!isCustomerPredictionTable(tableName)) {
      return;
    }
    JPA.runInTransaction(
        () -> JPA.em().createQuery("DELETE FROM LakeCustomerPrediction").executeUpdate());
  }

  private Statement createLakehouseStatement(Connection connection) throws SQLException {
    final Statement statement = connection.createStatement();
    statement.setFetchSize(EMPLOYEE_SYNC_BATCH_SIZE);
    configureDuckDbIcebergCatalog(statement);
    return statement;
  }

  private void configureDuckDbIcebergCatalog(Statement statement) throws SQLException {
    executeDuckDbCommand(statement, "INSTALL httpfs");
    executeDuckDbCommand(statement, "LOAD httpfs");
    executeDuckDbCommand(statement, "INSTALL iceberg");
    executeDuckDbCommand(statement, "LOAD iceberg");

    String endpoint = getPgDuckDbMinioEndpoint().replace("http://", "").replace("https://", "");
    executeDuckDbCommand(statement, "SET s3_endpoint='" + escapeSql(endpoint) + "'");
    executeDuckDbCommand(
        statement, "SET s3_access_key_id='" + escapeSql(getMinioAccessKey()) + "'");
    executeDuckDbCommand(
        statement, "SET s3_secret_access_key='" + escapeSql(getMinioSecretKey()) + "'");
    executeDuckDbCommand(statement, "SET s3_region='" + escapeSql(getIcebergS3Region()) + "'");
    executeDuckDbCommand(statement, "SET s3_use_ssl=false");
    executeDuckDbCommand(statement, "SET s3_url_style='path'");
    executeDuckDbCommand(
        statement,
        "ATTACH '"
            + escapeSql(getIcebergWarehouse())
            + "' AS "
            + sanitizeIdentifier(getIcebergCatalogAlias())
            + " (TYPE iceberg, ENDPOINT '"
            + escapeSql(getPgDuckDbIcebergRestUri())
            + "', AUTHORIZATION_TYPE 'none')",
        true);
  }

  private void executeDuckDbCommand(Statement statement, String command) throws SQLException {
    executeDuckDbCommand(statement, command, false);
  }

  private void executeDuckDbCommand(
      Statement statement, String command, boolean ignoreAlreadyAttached) throws SQLException {
    try {
      statement.execute("SELECT duckdb.raw_query('" + escapeSql(command) + "')");
    } catch (SQLException e) {
      String message = e.getMessage() == null ? "" : e.getMessage().toLowerCase(Locale.ROOT);
      if (ignoreAlreadyAttached && message.contains("already")) {
        return;
      }
      throw e;
    }
  }

  private String buildLakehouseQuery(String metadataPath, int limit) {
    if (metadataPath == null || metadataPath.isBlank()) {
      throw new IllegalArgumentException("Metadata path is required.");
    }

    String normalizedPath = metadataPath.trim().toLowerCase(Locale.ROOT);
    String sourceQuery;
    if (normalizedPath.startsWith(ICEBERG_TABLE_PREFIX)) {
      sourceQuery =
          "SELECT * FROM "
              + sanitizeQualifiedIdentifier(
                  metadataPath.trim().substring(ICEBERG_TABLE_PREFIX.length()))
              + buildLimitClause(limit);
    } else if (normalizedPath.endsWith(".csv")) {
      sourceQuery =
          "SELECT * FROM read_csv_auto(''"
              + escapeSql(metadataPath)
              + "'', HEADER=TRUE, SAMPLE_SIZE=-1)"
              + buildLimitClause(limit);
    } else if (normalizedPath.endsWith(".parquet")) {
      sourceQuery =
          "SELECT * FROM read_parquet(''"
              + escapeSql(metadataPath)
              + "'')"
              + buildLimitClause(limit);
    } else {
      sourceQuery =
          "SELECT * FROM iceberg_scan(''"
              + escapeSql(metadataPath)
              + "'')"
              + buildLimitClause(limit);
    }

    return "SELECT * FROM duckdb.query('" + sourceQuery + "')";
  }

  private String getStagingIcebergTableReference(String tableName) {
    String modelName =
        isCustomerPredictionTable(tableName) ? "stg_customer_profile" : "stg_employee";
    return buildIcebergTableReference(getIcebergStagingSchema(), modelName);
  }

  private String getCustomerPredictionsIcebergTableReference() {
    return buildIcebergTableReference(getIcebergAnalyticsSchema(), "customer_predictions");
  }

  private String buildIcebergTableReference(String schemaName, String tableName) {
    return ICEBERG_TABLE_PREFIX
        + sanitizeIdentifier(getIcebergCatalogAlias())
        + "."
        + sanitizeIdentifier(schemaName)
        + "."
        + sanitizeIdentifier(tableName);
  }

  private String sanitizeQualifiedIdentifier(String tableReference) {
    if (tableReference == null || tableReference.isBlank()) {
      throw new IllegalArgumentException("Iceberg table reference is required.");
    }
    String[] parts = tableReference.trim().split("\\.");
    if (parts.length != 3) {
      throw new IllegalArgumentException(
          "Iceberg table reference must be catalog.schema.table: " + tableReference);
    }
    List<String> sanitizedParts = new ArrayList<>();
    for (String part : parts) {
      sanitizedParts.add(sanitizeIdentifier(part));
    }
    return String.join(".", sanitizedParts);
  }

  private String sanitizeIdentifier(String identifier) {
    if (identifier == null || !TABLE_NAME_PATTERN.matcher(identifier.trim()).matches()) {
      throw new IllegalArgumentException("Invalid identifier: " + identifier);
    }
    return identifier.trim();
  }

  private Map<String, Integer> resolveColumnIndexes(ResultSetMetaData metaData)
      throws SQLException {
    final Map<String, Integer> columnIndexes = new HashMap<>();
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      columnIndexes.put(normalizeColumnName(metaData.getColumnLabel(i)), i);
    }
    return columnIndexes;
  }

  private String abbreviateMessage(String message, int maxLength) {
    if (message == null || message.isBlank()) {
      return "Profile sync failed.";
    }
    if (message.length() <= maxLength) {
      return message;
    }
    return message.substring(0, maxLength - 3) + "...";
  }

  private boolean isProfileCompatibleTable(String metadataPath) throws SQLException {
    try (Connection connection =
            DriverManager.getConnection(
                getPgDuckDbJdbcUrl(), getPgDuckDbUsername(), getPgDuckDbPassword());
        Statement statement = createLakehouseStatement(connection);
        ResultSet resultSet = statement.executeQuery(buildLakehouseQuery(metadataPath, 1))) {
      final Map<String, Integer> columnIndexes = resolveColumnIndexes(resultSet.getMetaData());
      return hasAnyColumn(columnIndexes, "employeeId", "employee_id", "id")
          && hasAnyColumn(columnIndexes, "name", "employeeName", "employee_name")
          && hasAnyColumn(columnIndexes, "department", "dept")
          && hasAnyColumn(columnIndexes, "age", "employeeAge")
          && hasAnyColumn(columnIndexes, "salary", "employeeSalary")
          && hasAnyColumn(columnIndexes, "status", "employeeStatus");
    }
  }

  private boolean hasAnyColumn(Map<String, Integer> columnIndexes, String... candidateColumns) {
    for (String candidateColumn : candidateColumns) {
      if (columnIndexes.containsKey(normalizeColumnName(candidateColumn))) {
        return true;
      }
    }
    return false;
  }

  private Integer getRequiredIntegerValue(
      ResultSet resultSet, Map<String, Integer> columnIndexes, String... candidateColumns)
      throws SQLException {
    final String value = getRequiredRowValue(resultSet, columnIndexes, candidateColumns);
    try {
      return Integer.valueOf(value);
    } catch (NumberFormatException e) {
      throw new IllegalStateException(
          "Invalid integer value for columns: " + String.join(", ", candidateColumns), e);
    }
  }

  private BigDecimal getRequiredDecimalValue(
      ResultSet resultSet, Map<String, Integer> columnIndexes, String... candidateColumns)
      throws SQLException {
    final String value = getRequiredRowValue(resultSet, columnIndexes, candidateColumns);
    try {
      return new BigDecimal(value);
    } catch (NumberFormatException e) {
      throw new IllegalStateException(
          "Invalid decimal value for columns: " + String.join(", ", candidateColumns), e);
    }
  }

  private BigDecimal getOptionalDecimalValue(
      ResultSet resultSet, Map<String, Integer> columnIndexes, String... candidateColumns)
      throws SQLException {
    final String value = getOptionalRowValue(resultSet, columnIndexes, candidateColumns);
    if (value == null) {
      return null;
    }
    try {
      return new BigDecimal(value);
    } catch (NumberFormatException e) {
      throw new IllegalStateException(
          "Invalid decimal value for columns: " + String.join(", ", candidateColumns), e);
    }
  }

  private String getRequiredRowValue(
      ResultSet resultSet, Map<String, Integer> columnIndexes, String... candidateColumns)
      throws SQLException {
    for (String candidateColumn : candidateColumns) {
      final Integer columnIndex = columnIndexes.get(normalizeColumnName(candidateColumn));
      if (columnIndex == null) {
        continue;
      }

      final Object value = resultSet.getObject(columnIndex);
      if (value != null) {
        final String text = String.valueOf(value).trim();
        if (!text.isEmpty()) {
          return text;
        }
      }
    }

    throw new IllegalStateException(
        "Missing required employee column. Expected one of: "
            + String.join(", ", candidateColumns));
  }

  private String getOptionalRowValue(
      ResultSet resultSet, Map<String, Integer> columnIndexes, String... candidateColumns)
      throws SQLException {
    for (String candidateColumn : candidateColumns) {
      final Integer columnIndex = columnIndexes.get(normalizeColumnName(candidateColumn));
      if (columnIndex == null) {
        continue;
      }

      final Object value = resultSet.getObject(columnIndex);
      if (value != null) {
        final String text = String.valueOf(value).trim();
        if (!text.isEmpty()) {
          return text;
        }
      }
    }

    return null;
  }

  private String normalizeColumnName(String value) {
    return value.replaceAll("[^A-Za-z0-9]", "").toLowerCase(Locale.ROOT);
  }

  private List<Map<String, Object>> mapRows(ResultSet resultSet) throws SQLException {
    final ResultSetMetaData metaData = resultSet.getMetaData();
    final int columnCount = metaData.getColumnCount();
    final List<Map<String, Object>> rows = new ArrayList<>();

    while (resultSet.next()) {
      final Map<String, Object> row = new LinkedHashMap<>();
      for (int i = 1; i <= columnCount; i++) {
        row.put(metaData.getColumnLabel(i), resultSet.getObject(i));
      }
      rows.add(row);
    }

    return rows;
  }

  private String validateCsv(File csv, String tableName) {
    if (csv == null || !csv.isFile()) {
      throw new IllegalArgumentException("CSV file is required.");
    }
    return validateTableName(tableName);
  }

  private String validateTableName(String tableName) {
    if (tableName == null || tableName.isBlank()) {
      throw new IllegalArgumentException("Table name is required.");
    }
    final String normalizedTableName = tableName.trim();
    if (!TABLE_NAME_PATTERN.matcher(normalizedTableName).matches()) {
      throw new IllegalArgumentException(
          "Table name may contain only letters, numbers, and underscores.");
    }
    return normalizedTableName;
  }

  private String triggerPipeline(String tableName) throws IOException, InterruptedException {
    return triggerPipeline(tableName, false);
  }

  private String triggerPipeline(String tableName, boolean wait)
      throws IOException, InterruptedException {
    final String normalizedTableName = validateTableName(tableName);
    String queueUrl = enqueueJenkinsBuild(normalizedTableName);
    if (wait) {
      waitForJenkinsBuild(queueUrl);
    }
    return queueUrl;
  }

  private void ensureSuccess(HttpResponse<String> response, String message) {
    if (response.statusCode() / 100 != 2) {
      throw new IllegalStateException(
          message + ": " + response.statusCode() + " " + response.body());
    }
  }

  private String enqueueJenkinsBuild(String tableName) throws IOException, InterruptedException {
    String jobUrl = getJenkinsJobUrl();
    if (jobUrl.isBlank()) {
      throw new IllegalStateException("Jenkins job URL is not configured.");
    }

    Map<String, String> headers = getJenkinsCrumbHeaders();
    String triggerUrl = jobUrl + "/buildWithParameters";
    List<String> queryParts = new ArrayList<>();
    queryParts.add("TABLE_NAME=" + URLEncoder.encode(tableName, StandardCharsets.UTF_8));

    String buildToken = getJenkinsBuildToken();
    boolean useBuildToken = headers.isEmpty() && !buildToken.isBlank();
    if (useBuildToken) {
      queryParts.add("token=" + URLEncoder.encode(buildToken, StandardCharsets.UTF_8));
    }
    triggerUrl = triggerUrl + "?" + String.join("&", queryParts);

    if (isWindowsHost() && !headers.isEmpty()) {
      LOG.info("Triggering Jenkins via PowerShell for {}", jobUrl);
      return triggerJenkinsViaPowerShell(triggerUrl);
    }

    HttpRequest.Builder requestBuilder =
        HttpRequest.newBuilder()
            .uri(URI.create(triggerUrl))
            .timeout(Duration.ofSeconds(30))
            .POST(HttpRequest.BodyPublishers.noBody());
    headers.forEach(requestBuilder::header);

    HttpResponse<String> response =
        httpClient.send(
            requestBuilder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    if (response.statusCode() != 200
        && response.statusCode() != 201
        && response.statusCode() != 202) {
      throw new IllegalStateException(
          "Jenkins pipeline trigger failed: "
              + response.statusCode()
              + " "
              + response.body()
              + " [jobUrl="
              + jobUrl
              + ", user="
              + (getJenkinsUser().isBlank() ? "<blank>" : getJenkinsUser())
              + ", userSource="
              + describeSettingSource("axelor.lakehouse.jenkins.user", "JENKINS_USER")
              + ", authPresent="
              + !headers.isEmpty()
              + ", apiToken="
              + fingerprintSecret(getJenkinsApiToken())
              + ", apiTokenSource="
              + describeSettingSource("axelor.lakehouse.jenkins.api-token", "JENKINS_API_TOKEN")
              + ", buildTokenUsed="
              + useBuildToken
              + "]");
    }

    return response
        .headers()
        .firstValue("Location")
        .orElseThrow(() -> new IllegalStateException("Jenkins did not return a queue location."));
  }

  private String triggerJenkinsViaPowerShell(String triggerUrl)
      throws IOException, InterruptedException {
    Process process =
        new ProcessBuilder(
                "powershell",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                buildPowerShellJenkinsTriggerScript(triggerUrl))
            .redirectErrorStream(true)
            .start();

    String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      throw new IllegalStateException(
          "PowerShell Jenkins trigger failed: " + normalizeProcessOutput(output));
    }

    String queueUrl =
        output
            .lines()
            .map(String::trim)
            .filter(line -> !line.isBlank())
            .reduce((first, second) -> second)
            .orElse("");
    if (queueUrl.isBlank()) {
      throw new IllegalStateException(
          "PowerShell Jenkins trigger did not return a queue location.");
    }
    return queueUrl;
  }

  private String buildPowerShellJenkinsTriggerScript(String triggerUrl) {
    return "$pair = '"
        + escapePowerShellSingleQuoted(getJenkinsUser())
        + ":"
        + escapePowerShellSingleQuoted(getJenkinsApiToken())
        + "'; "
        + "$bytes = [System.Text.Encoding]::UTF8.GetBytes($pair); "
        + "$basic = [Convert]::ToBase64String($bytes); "
        + "$resp = Invoke-WebRequest -UseBasicParsing -Method Post "
        + "-Headers @{ Authorization = ('Basic ' + $basic) } '"
        + escapePowerShellSingleQuoted(triggerUrl)
        + "'; "
        + "$location = $resp.Headers['Location']; "
        + "if (-not $location) { $location = $resp.BaseResponse.Headers['Location']; } "
        + "if ($location -is [array]) { $location = $location[0]; } "
        + "if (-not $location) { Write-Error 'Jenkins did not return a queue location.'; exit 1 } "
        + "Write-Output $location";
  }

  private String escapePowerShellSingleQuoted(String value) {
    return value.replace("'", "''");
  }

  private boolean isWindowsHost() {
    return System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");
  }

  private String normalizeProcessOutput(String output) {
    String normalized = output == null ? "" : output.trim();
    return normalized.isBlank() ? "<no output>" : normalized;
  }

  private void waitForJenkinsBuild(String queueUrl) throws IOException, InterruptedException {
    long deadline = System.currentTimeMillis() + getJenkinsWaitTimeoutSeconds() * 1000L;
    int buildNumber = waitForQueueExecutable(queueUrl, deadline);
    waitForBuildCompletion(buildNumber, deadline);
  }

  private int waitForQueueExecutable(String queueUrl, long deadline)
      throws IOException, InterruptedException {
    while (System.currentTimeMillis() < deadline) {
      HttpResponse<String> response =
          sendAuthenticatedJenkinsRequest(
              queueUrl + "/api/json", "GET", HttpRequest.BodyPublishers.noBody());
      ensureSuccess(response, "Unable to inspect Jenkins queue item");
      JsonNode payload = OBJECT_MAPPER.readTree(response.body());
      JsonNode executable = payload.path("executable");
      if (!executable.isMissingNode() && executable.has("number")) {
        return executable.path("number").asInt();
      }
      if (payload.path("cancelled").asBoolean(false)) {
        throw new IllegalStateException("Jenkins queue item was cancelled.");
      }
      Thread.sleep(getJenkinsPollIntervalMillis());
    }

    throw new IllegalStateException("Timed out waiting for Jenkins job to start.");
  }

  private void waitForBuildCompletion(int buildNumber, long deadline)
      throws IOException, InterruptedException {
    String buildApiUrl = getJenkinsJobUrl() + "/" + buildNumber + "/api/json";
    while (System.currentTimeMillis() < deadline) {
      HttpResponse<String> response =
          sendAuthenticatedJenkinsRequest(buildApiUrl, "GET", HttpRequest.BodyPublishers.noBody());
      ensureSuccess(response, "Unable to inspect Jenkins build result");
      JsonNode payload = OBJECT_MAPPER.readTree(response.body());
      if (!payload.path("building").asBoolean(false)) {
        String result = payload.path("result").asText("UNKNOWN");
        if (!"SUCCESS".equalsIgnoreCase(result)) {
          throw new IllegalStateException("Jenkins pipeline failed with result: " + result);
        }
        return;
      }
      Thread.sleep(getJenkinsPollIntervalMillis());
    }

    throw new IllegalStateException("Timed out waiting for Jenkins build to finish.");
  }

  private HttpResponse<String> sendAuthenticatedJenkinsRequest(
      String url, String method, HttpRequest.BodyPublisher bodyPublisher)
      throws IOException, InterruptedException {
    HttpRequest.Builder requestBuilder =
        HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofSeconds(30))
            .method(method, bodyPublisher)
            .header("Accept", "application/json");
    getJenkinsAuthHeaders().forEach(requestBuilder::header);
    return httpClient.send(
        requestBuilder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
  }

  private Map<String, String> getJenkinsCrumbHeaders() throws IOException, InterruptedException {
    Map<String, String> headers = new LinkedHashMap<>(getJenkinsAuthHeaders());
    // Jenkins accepts API-token authenticated build triggers without a CSRF crumb.
    // Skipping the crumb lookup avoids 401 failures on crumbIssuer for secured setups.
    if (!headers.isEmpty()) {
      return headers;
    }
    if (getJenkinsUrl().isBlank()) {
      return headers;
    }

    HttpRequest.Builder requestBuilder =
        HttpRequest.newBuilder()
            .uri(URI.create(getJenkinsUrl() + "/crumbIssuer/api/json"))
            .timeout(Duration.ofSeconds(30))
            .GET()
            .header("Accept", "application/json");
    headers.forEach(requestBuilder::header);

    HttpResponse<String> response =
        httpClient.send(
            requestBuilder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    if (response.statusCode() == 404 || response.statusCode() == 403) {
      return headers;
    }

    ensureSuccess(response, "Unable to fetch Jenkins crumb");
    Map<String, Object> crumbPayload =
        OBJECT_MAPPER.readValue(response.body(), new TypeReference<Map<String, Object>>() {});
    Object crumbField = crumbPayload.get("crumbRequestField");
    Object crumbValue = crumbPayload.get("crumb");
    if (crumbField != null && crumbValue != null) {
      headers.put(String.valueOf(crumbField), String.valueOf(crumbValue));
    }
    return headers;
  }

  private Map<String, String> getJenkinsAuthHeaders() {
    String user = getJenkinsUser();
    String apiToken = getJenkinsApiToken();
    if (user.isBlank() || apiToken.isBlank()) {
      return Map.of();
    }

    String encodedAuth =
        Base64.getEncoder()
            .encodeToString((user + ":" + apiToken).getBytes(StandardCharsets.UTF_8));
    return Map.of("Authorization", "Basic " + encodedAuth);
  }

  private String escapeSql(String value) {
    return value.replace("'", "''");
  }

  private String buildLimitClause(int limit) {
    return limit > 0 ? " LIMIT " + limit : "";
  }

  private String getMinioEndpoint() {
    return getSetting(
        "axelor.lakehouse.minio.endpoint",
        "AXELOR_LAKEHOUSE_MINIO_ENDPOINT",
        "http://localhost:9000");
  }

  private String getPgDuckDbMinioEndpoint() {
    return getSetting(
        "axelor.lakehouse.pgduckdb.minio.endpoint",
        "AXELOR_LAKEHOUSE_PGDUCKDB_MINIO_ENDPOINT",
        getMinioEndpoint());
  }

  private String getMinioAccessKey() {
    return getSetting("axelor.lakehouse.minio.access-key", "MINIO_ACCESS_KEY", "admin");
  }

  private String getMinioSecretKey() {
    return getSetting("axelor.lakehouse.minio.secret-key", "MINIO_SECRET_KEY", "password123");
  }

  private String getMinioRawBucket() {
    return getSetting("axelor.lakehouse.minio.raw-bucket", "MINIO_RAW_BUCKET", "lake-raw");
  }

  private String getMinioStagingBucket() {
    return getSetting(
        "axelor.lakehouse.minio.staging-bucket", "MINIO_STAGING_BUCKET", "lake-staging");
  }

  private String getMinioCuratedBucket() {
    return getSetting(
        "axelor.lakehouse.minio.curated-bucket", "MINIO_CURATED_BUCKET", "lake-curated");
  }

  private String getMinioAnalyticsBucket() {
    return getSetting(
        "axelor.lakehouse.minio.analytics-bucket", "MINIO_ANALYTICS_BUCKET", "lake-analytics");
  }

  private String getMinioModelsBucket() {
    return getSetting("axelor.lakehouse.minio.models-bucket", "MINIO_MODELS_BUCKET", "lake-models");
  }

  private String getIcebergCatalogAlias() {
    return getSetting(
        "axelor.lakehouse.iceberg.catalog-alias",
        "AXELOR_LAKEHOUSE_ICEBERG_CATALOG_ALIAS",
        "iceberg_lake");
  }

  private String getIcebergRestUri() {
    return getSetting(
        "axelor.lakehouse.iceberg.rest-uri",
        "AXELOR_LAKEHOUSE_ICEBERG_REST_URI",
        "http://localhost:19120/iceberg/main");
  }

  private String getPgDuckDbIcebergRestUri() {
    return getSetting(
        "axelor.lakehouse.pgduckdb.iceberg.rest-uri",
        "AXELOR_LAKEHOUSE_PGDUCKDB_ICEBERG_REST_URI",
        getIcebergRestUri());
  }

  private String getIcebergWarehouse() {
    return getSetting(
        "axelor.lakehouse.iceberg.warehouse", "AXELOR_LAKEHOUSE_ICEBERG_WAREHOUSE", "warehouse");
  }

  private String getIcebergStagingSchema() {
    return getSetting(
        "axelor.lakehouse.iceberg.staging-schema",
        "AXELOR_LAKEHOUSE_ICEBERG_STAGING_SCHEMA",
        "lake_staging");
  }

  private String getIcebergAnalyticsSchema() {
    return getSetting(
        "axelor.lakehouse.iceberg.analytics-schema",
        "AXELOR_LAKEHOUSE_ICEBERG_ANALYTICS_SCHEMA",
        "lake_analytics");
  }

  private String getIcebergS3Region() {
    return getSetting(
        "axelor.lakehouse.iceberg.s3-region", "AXELOR_LAKEHOUSE_ICEBERG_S3_REGION", "us-east-1");
  }

  private String getJenkinsUrl() {
    return getSetting("axelor.lakehouse.jenkins.url", "JENKINS_URL", "http://localhost:8080");
  }

  private String getJenkinsJobUrl() {
    String configuredValue = getSetting("axelor.lakehouse.jenkins.job-url", "JENKINS_JOB_URL", "");
    if (configuredValue != null && !configuredValue.isBlank()) {
      return configuredValue;
    }
    return getJenkinsUrl() + "/job/open-suite-lake-pipeline";
  }

  private String getJenkinsUser() {
    return getSetting("axelor.lakehouse.jenkins.user", "JENKINS_USER", "");
  }

  private String getJenkinsApiToken() {
    return getSetting("axelor.lakehouse.jenkins.api-token", "JENKINS_API_TOKEN", "");
  }

  private String getJenkinsBuildToken() {
    return getSetting("axelor.lakehouse.jenkins.build-token", "JENKINS_BUILD_TOKEN", "");
  }

  private long getJenkinsWaitTimeoutSeconds() {
    return Long.parseLong(
        getSetting(
            "axelor.lakehouse.jenkins.wait-timeout-seconds",
            "JENKINS_WAIT_TIMEOUT_SECONDS",
            "600"));
  }

  private long getJenkinsPollIntervalMillis() {
    double seconds =
        Double.parseDouble(
            getSetting(
                "axelor.lakehouse.jenkins.poll-interval-seconds",
                "JENKINS_POLL_INTERVAL_SECONDS",
                "2"));
    return Math.max(500L, (long) (seconds * 1000));
  }

  private String getPgDuckDbJdbcUrl() {
    return getSetting(
        "axelor.lakehouse.pgduckdb.jdbc-url",
        "AXELOR_LAKEHOUSE_PGDUCKDB_JDBC_URL",
        "jdbc:postgresql://localhost:5433/analytics");
  }

  private String getPgDuckDbUsername() {
    return getSetting(
        "axelor.lakehouse.pgduckdb.username", "AXELOR_LAKEHOUSE_PGDUCKDB_USERNAME", "postgres");
  }

  private String getPgDuckDbPassword() {
    return getSetting(
        "axelor.lakehouse.pgduckdb.password", "AXELOR_LAKEHOUSE_PGDUCKDB_PASSWORD", "duckdb");
  }

  private String getSetting(String propertyKey, String envKey, String defaultValue) {
    String systemValue = System.getProperty(propertyKey);
    if (systemValue != null) {
      String trimmed = systemValue.trim();
      if (!trimmed.isBlank()) {
        return trimmed;
      }
    }

    try {
      String appValue = AppSettings.get().get(propertyKey);
      if (appValue != null) {
        String trimmed = appValue.trim();
        if (!trimmed.isBlank()) {
          return trimmed;
        }
      }
    } catch (Exception ignored) {
      // Ignore settings bootstrap issues and continue to env/default fallback.
    }

    String envValue = System.getenv(envKey);
    if (envValue != null) {
      String trimmed = envValue.trim();
      if (!trimmed.isBlank()) {
        return trimmed;
      }
    }
    return defaultValue;
  }

  private String describeSettingSource(String propertyKey, String envKey) {
    String systemValue = System.getProperty(propertyKey);
    if (systemValue != null && !systemValue.trim().isBlank()) {
      return "system";
    }

    try {
      String appValue = AppSettings.get().get(propertyKey);
      if (appValue != null && !appValue.trim().isBlank()) {
        return "app";
      }
    } catch (Exception ignored) {
      // Ignore settings bootstrap issues and continue to env/default fallback.
    }

    String envValue = System.getenv(envKey);
    if (envValue != null && !envValue.trim().isBlank()) {
      return "env";
    }
    return "default";
  }

  private String fingerprintSecret(String value) {
    if (value == null) {
      return "<null>";
    }
    String trimmed = value.trim();
    if (trimmed.isBlank()) {
      return "<blank>";
    }
    int length = trimmed.length();
    String suffix = trimmed.substring(Math.max(0, length - 4));
    return "len=" + length + ",suffix=" + suffix;
  }
}
