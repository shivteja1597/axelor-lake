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

import com.axelor.concurrent.ContextAware;
import com.axelor.db.JPA;
import com.axelor.db.tenants.TenantResolver;
import com.axelor.lake.db.LakeDepartment;
import com.axelor.lake.db.LakeEmployee;
import com.axelor.lake.db.LakeRole;
import com.axelor.lake.db.LakeStatus;
import com.axelor.lake.db.LakehouseTable;
import com.axelor.lake.db.repo.LakehouseTableRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.persist.Transactional;
import jakarta.persistence.EntityManager;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;
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
  private static final Pattern TABLE_NAME_PATTERN = Pattern.compile("^[A-Za-z0-9_]+$");

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

    final JsonNode payload = sendMultipartUpload(csv, normalizedTableName);
    final String metadataPath = getRequiredText(payload, "metadata_path");
    final long uploadedRowCount = payload.path("row_count").asLong(0L);

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

    final boolean profileCompatible = isProfileCompatibleTable(metadataPath);
    if (profileCompatible) {
      record.setSyncStatus(SYNC_STATUS_PENDING);
      record.setLastSyncAt(null);
      record.setSyncMessage("Profile data sync is running in background.");
    } else {
      record.setSyncStatus(SYNC_STATUS_READY);
      record.setLastSyncAt(LocalDateTime.now());
      record.setSyncMessage(null);
    }
    record = lakehouseTableRepository.save(record);

    scheduleEmployeesSyncIfNeeded(normalizedTableName, metadataPath, profileCompatible);

    return record;
  }

  @Override
  public void trainTelecomModel(String tableName) throws IOException, InterruptedException {
    invokeTelecomMlEndpoint("/ml/telecom-churn/train", tableName);
  }

  @Override
  public void predictTelecomData(String tableName) throws IOException, InterruptedException {
    invokeTelecomMlEndpoint("/ml/telecom-churn/predict", tableName);
  }

  @Override
  public void runCustomerRiskWorkflow(String tableName) throws IOException, InterruptedException {
    final String normalizedTableName = validateTableName(tableName);
    invokeCustomerMlEndpoint("/ml/customer-risk/trigger", normalizedTableName);
  }

  @Override
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
  public String getLatestMetadataPath(String tableName) throws IOException, InterruptedException {
    final String encodedTableName = URLEncoder.encode(tableName, StandardCharsets.UTF_8);
    final HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(getFastApiBaseUrl() + "/metadata/" + encodedTableName))
            .timeout(Duration.ofSeconds(20))
            .GET()
            .build();

    final HttpResponse<String> response =
        httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    ensureSuccess(response, "FastAPI metadata lookup failed");

    final JsonNode payload = OBJECT_MAPPER.readTree(response.body());
    return getRequiredText(payload, "metadata_path");
  }

  @Override
  public void deleteTable(String tableName) throws IOException, InterruptedException {
    final String normalizedTableName = validateTableName(tableName);
    final String encodedTableName = URLEncoder.encode(normalizedTableName, StandardCharsets.UTF_8);
    final HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(getFastApiBaseUrl() + "/tables/" + encodedTableName))
            .timeout(Duration.ofMinutes(2))
            .DELETE()
            .build();

    final HttpResponse<String> response =
        httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    ensureSuccess(response, "FastAPI lakehouse delete failed");
  }

  private JsonNode sendMultipartUpload(File csv, String tableName) throws IOException {
    final String boundary = "----LakehouseBoundary" + UUID.randomUUID();
    final String fileName = csv.getName();
    final String probedContentType = Files.probeContentType(csv.toPath());
    final String contentType = probedContentType != null ? probedContentType : "text/csv";

    final HttpURLConnection connection =
        (HttpURLConnection) URI.create(getFastApiBaseUrl() + "/upload").toURL().openConnection();
    connection.setConnectTimeout((int) Duration.ofSeconds(10).toMillis());
    connection.setReadTimeout((int) Duration.ofMinutes(15).toMillis());
    connection.setDoOutput(true);
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);
    connection.setChunkedStreamingMode(BUFFER_SIZE);

    try (OutputStream outputStream = connection.getOutputStream();
        BufferedInputStream fileStream =
            new BufferedInputStream(Files.newInputStream(csv.toPath()))) {
      writeFormField(outputStream, boundary, "table_name", tableName);
      writeFileField(outputStream, boundary, "file", fileName, contentType, fileStream);
      outputStream.write(("--" + boundary + "--\r\n").getBytes(StandardCharsets.UTF_8));
      outputStream.flush();
    }

    final int statusCode = connection.getResponseCode();
    final String responseBody = readResponseBody(connection, statusCode);
    if (statusCode / 100 != 2) {
      throw new IllegalStateException("FastAPI upload failed: " + statusCode + " " + responseBody);
    }

    return OBJECT_MAPPER.readTree(responseBody);
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

  private Statement createLakehouseStatement(Connection connection) throws SQLException {
    final Statement statement = connection.createStatement();
    statement.setFetchSize(EMPLOYEE_SYNC_BATCH_SIZE);
    return statement;
  }

  private String buildLakehouseQuery(String metadataPath, int limit) {
    if (metadataPath == null || metadataPath.isBlank()) {
      throw new IllegalArgumentException("Metadata path is required.");
    }

    return "SELECT * FROM duckdb.query('SELECT * FROM iceberg_scan(''"
        + escapeSql(metadataPath)
        + "'')"
        + buildLimitClause(limit)
        + "')";
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

  private void writeFormField(OutputStream outputStream, String boundary, String name, String value)
      throws IOException {
    outputStream.write(("--" + boundary + "\r\n").getBytes(StandardCharsets.UTF_8));
    outputStream.write(
        ("Content-Disposition: form-data; name=\"" + name + "\"\r\n\r\n")
            .getBytes(StandardCharsets.UTF_8));
    outputStream.write(value.getBytes(StandardCharsets.UTF_8));
    outputStream.write("\r\n".getBytes(StandardCharsets.UTF_8));
  }

  private void writeFileField(
      OutputStream outputStream,
      String boundary,
      String fieldName,
      String fileName,
      String contentType,
      InputStream fileStream)
      throws IOException {
    outputStream.write(("--" + boundary + "\r\n").getBytes(StandardCharsets.UTF_8));
    outputStream.write(
        ("Content-Disposition: form-data; name=\""
                + fieldName
                + "\"; filename=\""
                + fileName
                + "\"\r\n")
            .getBytes(StandardCharsets.UTF_8));
    outputStream.write(
        ("Content-Type: " + contentType + "\r\n\r\n").getBytes(StandardCharsets.UTF_8));

    final byte[] buffer = new byte[BUFFER_SIZE];
    int bytesRead;
    while ((bytesRead = fileStream.read(buffer)) != -1) {
      outputStream.write(buffer, 0, bytesRead);
    }
    outputStream.write("\r\n".getBytes(StandardCharsets.UTF_8));
  }

  private String readResponseBody(HttpURLConnection connection, int statusCode) throws IOException {
    final InputStream inputStream =
        statusCode >= 400 ? connection.getErrorStream() : connection.getInputStream();
    if (inputStream == null) {
      return "";
    }

    try (InputStream responseStream = inputStream;
        ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
      final byte[] bytes = new byte[BUFFER_SIZE];
      int bytesRead;
      while ((bytesRead = responseStream.read(bytes)) != -1) {
        buffer.write(bytes, 0, bytesRead);
      }
      return buffer.toString(StandardCharsets.UTF_8);
    }
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

  private void triggerPipeline(String tableName) throws IOException, InterruptedException {
    triggerPipeline(tableName, false);
  }

  private void triggerPipeline(String tableName, boolean wait)
      throws IOException, InterruptedException {
    final String normalizedTableName = validateTableName(tableName);
    final HttpRequest request =
        HttpRequest.newBuilder()
            .uri(
                URI.create(
                    getFastApiBaseUrl()
                        + "/pipeline/run?wait="
                        + wait
                        + "&table_name="
                        + URLEncoder.encode(normalizedTableName, StandardCharsets.UTF_8)))
            .timeout(Duration.ofMinutes(15))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build();

    final HttpResponse<String> response =
        httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    ensureSuccess(response, "FastAPI pipeline trigger failed");
  }

  private void invokeTelecomMlEndpoint(String endpointPath, String tableName)
      throws IOException, InterruptedException {
    final String normalizedTableName = validateTableName(tableName);
    final String encodedTableName = URLEncoder.encode(normalizedTableName, StandardCharsets.UTF_8);
    final HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(getFastApiBaseUrl() + endpointPath + "?table_name=" + encodedTableName))
            .timeout(Duration.ofMinutes(15))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build();

    final HttpResponse<String> response =
        httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    ensureSuccess(response, "FastAPI telecom ML request failed");
  }

  private void invokeCustomerMlEndpoint(String endpointPath, String tableName)
      throws IOException, InterruptedException {
    final String normalizedTableName = validateTableName(tableName);
    final String encodedTableName = URLEncoder.encode(normalizedTableName, StandardCharsets.UTF_8);
    final HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(getFastApiBaseUrl() + endpointPath + "?table_name=" + encodedTableName))
            .timeout(Duration.ofMinutes(15))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build();

    final HttpResponse<String> response =
        httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    ensureSuccess(response, "FastAPI customer ML request failed");
  }

  private void ensureSuccess(HttpResponse<String> response, String message) {
    if (response.statusCode() / 100 != 2) {
      throw new IllegalStateException(
          message + ": " + response.statusCode() + " " + response.body());
    }
  }

  private String getRequiredText(JsonNode payload, String fieldName) {
    final String value = payload.path(fieldName).asText(null);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException("Missing required field: " + fieldName);
    }
    return value;
  }

  private String escapeSql(String value) {
    return value.replace("'", "''");
  }

  private String buildLimitClause(int limit) {
    return limit > 0 ? " LIMIT " + limit : "";
  }

  private String getFastApiBaseUrl() {
    return System.getProperty(
        "axelor.lakehouse.fastapi.base-url",
        System.getenv().getOrDefault("AXELOR_LAKEHOUSE_FASTAPI_BASE_URL", "http://localhost:8000"));
  }

  private String getPgDuckDbJdbcUrl() {
    return System.getProperty(
        "axelor.lakehouse.pgduckdb.jdbc-url",
        System.getenv()
            .getOrDefault(
                "AXELOR_LAKEHOUSE_PGDUCKDB_JDBC_URL",
                "jdbc:postgresql://localhost:5433/analytics"));
  }

  private String getPgDuckDbUsername() {
    return System.getProperty(
        "axelor.lakehouse.pgduckdb.username",
        System.getenv().getOrDefault("AXELOR_LAKEHOUSE_PGDUCKDB_USERNAME", "postgres"));
  }

  private String getPgDuckDbPassword() {
    return System.getProperty(
        "axelor.lakehouse.pgduckdb.password",
        System.getenv().getOrDefault("AXELOR_LAKEHOUSE_PGDUCKDB_PASSWORD", "duckdb"));
  }
}
