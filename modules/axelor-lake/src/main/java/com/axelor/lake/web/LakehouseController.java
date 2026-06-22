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
package com.axelor.lake.web;

import com.axelor.apps.base.service.exception.TraceBackService;
import com.axelor.db.JPA;
import com.axelor.i18n.I18n;
import com.axelor.inject.Beans;
import com.axelor.lake.db.LakeDepartment;
import com.axelor.lake.db.LakeRole;
import com.axelor.lake.db.LakeStatus;
import com.axelor.lake.db.LakehouseTable;
import com.axelor.lake.db.LakehouseUpload;
import com.axelor.lake.db.repo.LakehouseTableRepository;
import com.axelor.lake.service.LakehouseService;
import com.axelor.meta.MetaFiles;
import com.axelor.meta.db.MetaFile;
import com.axelor.meta.schema.actions.ActionView;
import com.axelor.rpc.ActionRequest;
import com.axelor.rpc.ActionResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import jakarta.persistence.Query;
import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LakehouseController {

  private static final Logger LOG = LoggerFactory.getLogger(LakehouseController.class);
  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

  private static final Map<String, String> GROUPABLE_COLUMNS =
      Map.of(
          "employeeId", "e.employee_id",
          "name", "e.name",
          "department", "e.department",
          "role", "e.role",
          "age", "e.age",
          "salary", "e.salary",
          "status", "e.status");

  private static final Map<String, List<String>> MULTI_FILTER_ALIASES =
      Map.of(
          "department", List.of("$departmentSet", "departmentSet", "department"),
          "role", List.of("$roleSet", "roleSet", "role"),
          "status", List.of("$statusSet", "statusSet", "status"));

  private static final Map<String, List<String>> SCALAR_FILTER_ALIASES =
      Map.of(
          "analysisReport", List.of("$analysisReport", "analysisReport"),
          "ageMin", List.of("$ageMin", "ageMin"),
          "ageMax", List.of("$ageMax", "ageMax"),
          "salaryMin", List.of("$salaryMin", "salaryMin"),
          "salaryMax", List.of("$salaryMax", "salaryMax"),
          "analysisPage", List.of("$analysisPage", "analysisPage", "analysisPageDisplay"),
          "analysisPageSize", List.of("$analysisPageSize", "analysisPageSize"),
          "customerPage", List.of("$customerPage", "customerPage", "customerPageDisplay"),
          "customerPageSize", List.of("$customerPageSize", "customerPageSize"));

  private static final int ANALYSIS_PAGE_SIZE = 100;
  private static final int ANALYSIS_MAX_PAGE_SIZE = 200;
  private static final String CUSTOMER_UPLOAD_TABLE_NAME = "customer_profile";
  private static final long AGENT_TIMEOUT_SECONDS = 90L;
  private static final Map<String, String> CUSTOMER_SCHEMA_ALIASES =
      Map.of("address_line2", "address_line_2");
  private static final Set<String> CUSTOMER_SCHEMA_COLUMNS =
      Set.of(
          "account_id",
          "site_id",
          "customer_first_name",
          "customer_last_name",
          "address_line1",
          "address_line_2",
          "city",
          "state",
          "country",
          "contract_status",
          "tenure_in_months",
          "days_left_in_recent_contract",
          "plan_type",
          "current_rmr",
          "billing_frequency",
          "payment_mode",
          "property_type",
          "fico_score",
          "customer_initial_contract_term",
          "remaining_life_time",
          "acv_segment",
          "projected_rmr_churn",
          "acv",
          "cost_to_serve",
          "rmr_difference_from_company_avg",
          "area_attrition");

  public void uploadCsv(ActionRequest request, ActionResponse response) {
    try {
      LakehouseUpload upload = request.getContext().asType(LakehouseUpload.class);
      MetaFile csvFile = upload.getCsvFile();

      if (csvFile == null) {
        response.setError(I18n.get("CSV file is required."));
        return;
      }

      Path csvPath = MetaFiles.getPath(csvFile);
      LakehouseTable table =
          Beans.get(LakehouseService.class)
              .uploadToLakehouse(csvPath.toFile(), upload.getTableName());

      response.setInfo(I18n.get("CSV uploaded to lakehouse successfully."));
      response.setCanClose(true);
      response.setReload(true);
      response.setView(
          ActionView.define(I18n.get("Lakehouse Table"))
              .model(LakehouseTable.class.getName())
              .add("form", "lakehouse-table-form")
              .add("grid", "lakehouse-table-grid")
              .param("forceTitle", "true")
              .param("forceEdit", "true")
              .context("_showRecord", String.valueOf(table.getId()))
              .map());
    } catch (Exception e) {
      TraceBackService.trace(response, e);
    }
  }

  public void uploadCustomerCsv(ActionRequest request, ActionResponse response) {
    try {
      LakehouseUpload upload = request.getContext().asType(LakehouseUpload.class);
      MetaFile csvFile = upload.getCsvFile();

      if (csvFile == null) {
        response.setError(I18n.get("CSV file is required."));
        return;
      }

      Path csvPath = MetaFiles.getPath(csvFile);
      Path normalizedCustomerCsvPath = normalizeCustomerCsv(csvPath);
      validateCustomerCsv(normalizedCustomerCsvPath);
      LakehouseTable table =
          Beans.get(LakehouseService.class)
              .uploadToLakehouse(
                  normalizedCustomerCsvPath.toFile(), CUSTOMER_UPLOAD_TABLE_NAME, true);

      response.setInfo(
          I18n.get(
              "Customer data uploaded successfully. Customer predictions are processing in background."));
      response.setCanClose(true);
      response.setReload(true);
      response.setView(
          ActionView.define(I18n.get("Customer Predictions"))
              .model("com.axelor.utils.db.Wizard")
              .add("form", "lake-customer-predictions-form")
              .param("forceTitle", "true")
              .context("customerTableId", String.valueOf(table.getId()))
              .map());
    } catch (IllegalArgumentException e) {
      response.setError(I18n.get(e.getMessage()));
    } catch (Exception e) {
      TraceBackService.trace(response, e);
    }
  }

  public void loadPreview(ActionRequest request, ActionResponse response) {
    try {
      LakehouseTable context = request.getContext().asType(LakehouseTable.class);
      if (context == null || context.getId() == null) {
        response.setValue("queryPreview", null);
        return;
      }

      LakehouseTable table = Beans.get(LakehouseTableRepository.class).find(context.getId());

      if (table == null) {
        response.setError(I18n.get("Lakehouse table not found."));
        return;
      }

      List<Map<String, Object>> rows =
          Beans.get(LakehouseService.class)
              .queryFromLakehouseByMetadataPath(
                  table.getMetadataPath(), LakehouseService.DEFAULT_PREVIEW_LIMIT);
      response.setValue("queryPreview", OBJECT_MAPPER.writeValueAsString(rows));
    } catch (Exception e) {
      TraceBackService.trace(response, e);
    }
  }

  public void openDataProfiling(ActionRequest request, ActionResponse response) {
    try {
      LakehouseTable context = request.getContext().asType(LakehouseTable.class);
      String title = I18n.get("Data Profiling");
      boolean syncScheduled = false;

      if (context != null && context.getId() != null) {
        LakehouseTable table = Beans.get(LakehouseTableRepository.class).find(context.getId());
        if (table == null) {
          response.setError(I18n.get("Lakehouse table not found."));
          return;
        }

        syncScheduled = ensureProfilingDataScheduled(table);
        title = I18n.get("Data Profiling") + " - " + table.getTableName();
      } else {
        syncScheduled = ensureProfilingDataScheduled(getLatestLakehouseTable());
      }

      refreshLookupFiltersFromLocalEmployees();
      if (syncScheduled) {
        response.setInfo(I18n.get("Profile data sync is running in background. Refresh shortly."));
      }

      response.setView(
          ActionView.define(title)
              .model("com.axelor.utils.db.Wizard")
              .add("form", "lake-data-profiling-form")
              .param("forceTitle", "true")
              .map());
    } catch (Exception e) {
      TraceBackService.trace(response, e);
    }
  }

  private LakehouseTable getLatestLakehouseTable() {
    return Beans.get(LakehouseTableRepository.class).all().order("-uploadedAt").fetchOne();
  }

  private boolean ensureProfilingDataScheduled(LakehouseTable table) throws SQLException {
    if (table == null || table.getMetadataPath() == null || table.getMetadataPath().isBlank()) {
      return false;
    }
    if ("Ready".equalsIgnoreCase(String.valueOf(table.getSyncStatus()))) {
      return false;
    }
    if (hasProfilingDataAvailable()
        && "Completed".equalsIgnoreCase(String.valueOf(table.getSyncStatus()))) {
      return false;
    }
    if ("Pending".equalsIgnoreCase(String.valueOf(table.getSyncStatus()))) {
      return false;
    }
    Beans.get(LakehouseService.class)
        .prepareProfilingDataAsync(table.getTableName(), table.getMetadataPath());
    return true;
  }

  public void deleteLakehouseTable(ActionRequest request, ActionResponse response) {
    try {
      LakehouseTable context = request.getContext().asType(LakehouseTable.class);
      if (context == null || context.getId() == null) {
        response.setError(I18n.get("Lakehouse table not found."));
        return;
      }

      LakehouseTableRepository repository = Beans.get(LakehouseTableRepository.class);
      LakehouseTable table = repository.find(context.getId());

      if (table == null) {
        response.setError(I18n.get("Lakehouse table not found."));
        return;
      }

      Beans.get(LakehouseService.class).deleteTable(table.getTableName());
      JPA.runInTransaction(() -> repository.remove(table));

      response.setNotify(I18n.get("Lakehouse table deleted from Axelor, Nessie, and MinIO."));
      response.setReload(true);
      response.setSignal("refresh-app", true);
    } catch (Exception e) {
      TraceBackService.trace(response, e);
    }
  }

  private void refreshLookupFiltersFromLocalEmployees() {
    if (!hasProfilingDataAvailable()) {
      return;
    }

    List<String> departments =
        JPA.em()
            .createQuery(
                "SELECT self.department FROM LakeEmployee self WHERE self.department IS NOT NULL",
                String.class)
            .getResultList();
    List<String> roles =
        JPA.em()
            .createQuery(
                "SELECT self.role FROM LakeEmployee self WHERE self.role IS NOT NULL", String.class)
            .getResultList();
    List<String> statuses =
        JPA.em()
            .createQuery(
                "SELECT self.status FROM LakeEmployee self WHERE self.status IS NOT NULL",
                String.class)
            .getResultList();

    Map<String, String> normalizedDepartments = normalizeLookupValues(departments);
    Map<String, String> normalizedRoles = normalizeLookupValues(roles);
    Map<String, String> normalizedStatuses = normalizeLookupValues(statuses);

    JPA.runInTransaction(
        () -> {
          JPA.em().createQuery("DELETE FROM LakeDepartment").executeUpdate();
          JPA.em().createQuery("DELETE FROM LakeRole").executeUpdate();
          JPA.em().createQuery("DELETE FROM LakeStatus").executeUpdate();

          persistLookupEntities(normalizedDepartments.values(), LakeDepartment.class);
          persistLookupEntities(normalizedRoles.values(), LakeRole.class);
          persistLookupEntities(normalizedStatuses.values(), LakeStatus.class);
        });
  }

  private Map<String, String> normalizeLookupValues(List<String> values) {
    Map<String, String> normalized = new TreeMap<>();
    for (String value : values) {
      String cleaned = normalizeLookupValue(value);
      if (cleaned == null) {
        continue;
      }
      normalized.putIfAbsent(cleaned.toLowerCase(Locale.ROOT), cleaned);
    }
    return normalized;
  }

  private String normalizeLookupValue(String value) {
    if (value == null) {
      return null;
    }
    String cleaned = value.trim().replaceAll("\\s+", " ");
    return cleaned.isEmpty() ? null : cleaned;
  }

  private <T> void persistLookupEntities(Iterable<String> values, Class<T> entityClass) {
    for (String value : values) {
      if (entityClass.equals(LakeDepartment.class)) {
        LakeDepartment entity = new LakeDepartment();
        entity.setName(value);
        JPA.em().persist(entity);
      } else if (entityClass.equals(LakeRole.class)) {
        LakeRole entity = new LakeRole();
        entity.setName(value);
        JPA.em().persist(entity);
      } else if (entityClass.equals(LakeStatus.class)) {
        LakeStatus entity = new LakeStatus();
        entity.setName(value);
        JPA.em().persist(entity);
      }
    }
  }

  public void nextAnalysisPage(ActionRequest request, ActionResponse response) {
    try {
      Map<String, Object> filters = extractFilters(request);
      Integer currentPage = getInteger(filters, "analysisPage");
      int nextPage = (currentPage == null ? 1 : currentPage) + 1;
      response.setValue("$analysisPage", nextPage);
      response.setValue("analysisPageDisplay", nextPage);
      response.setValue(
          "$analysisPageSize", normalizePageSize(getInteger(filters, "analysisPageSize")));
    } catch (Exception e) {
      TraceBackService.trace(response, e);
    }
  }

  public void prevAnalysisPage(ActionRequest request, ActionResponse response) {
    try {
      Map<String, Object> filters = extractFilters(request);
      Integer currentPage = getInteger(filters, "analysisPage");
      int page = currentPage == null ? 1 : currentPage;
      int prevPage = Math.max(1, page - 1);
      response.setValue("$analysisPage", prevPage);
      response.setValue("analysisPageDisplay", prevPage);
      response.setValue(
          "$analysisPageSize", normalizePageSize(getInteger(filters, "analysisPageSize")));
    } catch (Exception e) {
      TraceBackService.trace(response, e);
    }
  }

  public void loadColumnProfiles(ActionRequest request, ActionResponse response) {
    try {
      if (!hasProfilingDataAvailable()) {
        ensureProfilingDataScheduled(getLatestLakehouseTable());
        response.setData(List.of());
        return;
      }
      Map<String, Object> filters = extractFilters(request);
      String whereClause = buildWhereClause("dim_employee", filters);

      List<Map<String, Object>> rows = new ArrayList<>();
      rows.add(
          buildProfileRow(filters, whereClause, "employeeId", "String", "e.employee_id", true));
      rows.add(buildProfileRow(filters, whereClause, "name", "String", "e.name", false));
      rows.add(
          buildProfileRow(filters, whereClause, "department", "String", "e.department", false));
      rows.add(buildProfileRow(filters, whereClause, "role", "String", "e.role", false));
      rows.add(buildProfileRow(filters, whereClause, "age", "Double", "e.age", false));
      rows.add(buildProfileRow(filters, whereClause, "salary", "Double", "e.salary", false));
      rows.add(buildProfileRow(filters, whereClause, "status", "String", "e.status", false));

      response.setData(rows);
    } catch (Exception e) {
      TraceBackService.trace(response, e);
    }
  }

  public void loadCustomerPredictions(ActionRequest request, ActionResponse response) {
    try {
      Map<String, Object> filters = extractFilters(request);
      int pageSize = normalizePageSize(getInteger(filters, "customerPageSize"));
      long totalRows = countExternalReportRows("customer_predictions", Map.of(), null);
      int totalPages = totalRows <= 0 ? 1 : (int) Math.ceil((double) totalRows / pageSize);
      int page = normalizePage(getInteger(filters, "customerPage"), totalPages);
      int offset = (page - 1) * pageSize;
      List<Map<String, Object>> rows =
          executeExternalReportQuery("customer_predictions", Map.of(), offset, pageSize, null);

      for (Map<String, Object> row : rows) {
        row.put("_page", page);
        row.put("_totalRows", totalRows);
        row.put("_totalPages", totalPages);
        row.put("_reportName", "customer_predictions");
      }
      response.setData(rows);
    } catch (Exception e) {
      TraceBackService.trace(response, e);
    }
  }

  public void loadCustomerPredictionSummary(ActionRequest request, ActionResponse response) {
    try {
      Object[] row =
          (Object[])
              JPA.em()
                  .createQuery(
                      """
                SELECT
                  COUNT(self),
                  SUM(CASE WHEN LOWER(COALESCE(self.contractStatus, '')) = 'active' THEN 1 ELSE 0 END),
                  SUM(CASE WHEN self.riskSegment = 'High Risk' THEN 1 ELSE 0 END),
                  SUM(CASE WHEN self.riskSegment = 'Medium Risk' THEN 1 ELSE 0 END),
                  SUM(CASE WHEN self.riskSegment = 'Low Risk' THEN 1 ELSE 0 END),
                  AVG(self.churnRiskPercentage),
                  AVG(CASE WHEN self.riskSegment = 'High Risk' THEN self.churnRiskPercentage ELSE NULL END),
                  AVG(CASE WHEN self.riskSegment = 'Medium Risk' THEN self.churnRiskPercentage ELSE NULL END),
                  AVG(CASE WHEN self.riskSegment = 'Low Risk' THEN self.churnRiskPercentage ELSE NULL END)
                FROM LakeCustomerPrediction self
                """)
                  .getSingleResult();

      Map<String, Object> summary = new LinkedHashMap<>();
      summary.put("total_customers", toLong(row[0]));
      summary.put("active_customers", toLong(row[1]));
      summary.put("high_risk_customers", toLong(row[2]));
      summary.put("medium_risk_customers", toLong(row[3]));
      summary.put("low_risk_customers", toLong(row[4]));
      summary.put("predicted_churn_percentage", toDouble(row[5]));
      summary.put("high_risk_churn_percentage", toDouble(row[6]));
      summary.put("medium_risk_churn_percentage", toDouble(row[7]));
      summary.put("low_risk_churn_percentage", toDouble(row[8]));
      response.setData(List.of(summary));
    } catch (Exception e) {
      TraceBackService.trace(response, e);
    }
  }

  public void askLakeAgent(ActionRequest request, ActionResponse response) {
    Map<String, Object> context = request.getContext();
    String question = getAgentQuestion(context);
    if (question == null || question.isBlank() || "null".equals(question)) {
      response.setError(I18n.get("Please enter a question for the AI agent."));
      return;
    }

    try {
      String answer = runLakeAgent(question.trim());
      response.setValue("$agentAnswer", answer);
      response.setValue("agentAnswer", answer);
    } catch (Exception e) {
      LOG.error("Lakehouse AI agent request failed", e);
      response.setError(I18n.get(e.getMessage()));
    }
  }

  public void loadCustomerSegmentRiskMatrix(ActionRequest request, ActionResponse response) {
    try {
      List<Object[]> results =
          JPA.em()
              .createQuery(
                  """
                SELECT
                  COALESCE(NULLIF(TRIM(self.customerSegmentBucket), ''), 'Unknown Segment'),
                  SUM(CASE WHEN self.riskSegment = 'High Risk' THEN 1 ELSE 0 END),
                  SUM(CASE WHEN self.riskSegment = 'Medium Risk' THEN 1 ELSE 0 END),
                  SUM(CASE WHEN self.riskSegment = 'Low Risk' THEN 1 ELSE 0 END)
                FROM LakeCustomerPrediction self
                GROUP BY COALESCE(NULLIF(TRIM(self.customerSegmentBucket), ''), 'Unknown Segment')
                ORDER BY COALESCE(NULLIF(TRIM(self.customerSegmentBucket), ''), 'Unknown Segment')
                """)
              .getResultList();

      List<Map<String, Object>> rows = new ArrayList<>();
      for (Object[] result : results) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("customer_segment_bucket", result[0]);
        row.put("high_risk_count", toLong(result[1]));
        row.put("medium_risk_count", toLong(result[2]));
        row.put("low_risk_count", toLong(result[3]));
        rows.add(row);
      }
      response.setData(rows);
    } catch (Exception e) {
      TraceBackService.trace(response, e);
    }
  }

  public void loadCustomerPredictionsDb(ActionRequest request, ActionResponse response) {
    try {
      Map<String, Object> filters = extractFilters(request);
      Integer pageValue = getInteger(filters, "customerPage");
      Integer pageSizeValue = getInteger(filters, "customerPageSize");

      long totalRows =
          ((Number)
                  JPA.em()
                      .createQuery("SELECT COUNT(self) FROM LakeCustomerPrediction self")
                      .getSingleResult())
              .longValue();
      int pageSize = normalizePageSize(pageSizeValue);
      int totalPages = Math.max(1, (int) Math.ceil((double) totalRows / pageSize));
      int page = normalizePage(pageValue, totalPages);

      @SuppressWarnings("unchecked")
      List<com.axelor.lake.db.LakeCustomerPrediction> predictions =
          JPA.em()
              .createQuery(
                  "SELECT self FROM LakeCustomerPrediction self ORDER BY self.accountId ASC",
                  com.axelor.lake.db.LakeCustomerPrediction.class)
              .setFirstResult((page - 1) * pageSize)
              .setMaxResults(pageSize)
              .getResultList();

      List<Map<String, Object>> rows = new ArrayList<>();
      for (com.axelor.lake.db.LakeCustomerPrediction prediction : predictions) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("account_id", prediction.getAccountId());
        row.put("site_id", prediction.getSiteId());
        row.put("customer_name", prediction.getCustomerName());
        row.put("state", prediction.getState());
        row.put("contract_status", prediction.getContractStatus());
        row.put("plan_type", prediction.getPlanType());
        row.put("current_rmr", prediction.getCurrentRmr());
        row.put("churn_risk_percentage", prediction.getChurnRiskPercentage());
        row.put("risk_segment", prediction.getRiskSegment());
        row.put("_page", page);
        row.put("_totalRows", totalRows);
        row.put("_totalPages", totalPages);
        row.put("_reportName", "customer_predictions");
        rows.add(row);
      }
      response.setData(rows);
    } catch (Exception e) {
      TraceBackService.trace(response, e);
    }
  }

  public void loadCustomerPredictionSummaryDb(ActionRequest request, ActionResponse response) {
    loadCustomerPredictionSummary(request, response);
  }

  public void loadCustomerSegmentRiskMatrixDb(ActionRequest request, ActionResponse response) {
    loadCustomerSegmentRiskMatrix(request, response);
  }

  private boolean hasProfilingDataAvailable() {
    return getProfilingRowCount() > 0L;
  }

  private long getProfilingRowCount() {
    Number localCount =
        (Number)
            JPA.em().createQuery("SELECT COUNT(self) FROM LakeEmployee self").getSingleResult();
    return localCount == null ? 0L : localCount.longValue();
  }

  private int normalizePage(Integer requestedPage, int totalPages) {
    int page = requestedPage == null ? 1 : requestedPage;
    if (page < 1) {
      page = 1;
    }
    if (page > totalPages) {
      page = totalPages;
    }
    return page;
  }

  private int normalizePageSize(Integer requestedPageSize) {
    int pageSize = requestedPageSize == null ? ANALYSIS_PAGE_SIZE : requestedPageSize;
    if (pageSize < 1) {
      return ANALYSIS_PAGE_SIZE;
    }
    return Math.min(pageSize, ANALYSIS_MAX_PAGE_SIZE);
  }

  public void loadFilteredEmployees(ActionRequest request, ActionResponse response) {
    try {
      Map<String, Object> filters = extractFilters(request);
      String reportName = (String) filters.get("analysisReport");
      if (reportName == null || reportName.isBlank()) {
        reportName = "dim_employee";
      }

      int pageSize = normalizePageSize(getInteger(filters, "analysisPageSize"));
      long totalRows;
      List<Map<String, Object>> rows;
      int totalPages;
      int page;
      int offset;

      if ("dim_employee".equals(reportName)) {
        if (!hasProfilingDataAvailable()) {
          ensureProfilingDataScheduled(getLatestLakehouseTable());
          response.setData(List.of());
          return;
        }
        String whereClause = buildWhereClause(reportName, filters);
        String baseFrom = " FROM lake_lake_employee e WHERE " + whereClause;
        totalRows = getLongValue("SELECT COUNT(*)" + baseFrom, filters);
        totalPages = totalRows <= 0 ? 1 : (int) Math.ceil((double) totalRows / pageSize);
        page = normalizePage(getInteger(filters, "analysisPage"), totalPages);
        offset = (page - 1) * pageSize;
        rows = executeReportQuery(reportName, baseFrom, offset, pageSize, filters);
      } else {
        totalRows = countExternalReportRows(reportName, filters, null);
        totalPages = totalRows <= 0 ? 1 : (int) Math.ceil((double) totalRows / pageSize);
        page = normalizePage(getInteger(filters, "analysisPage"), totalPages);
        offset = (page - 1) * pageSize;
        rows = executeExternalReportQuery(reportName, filters, offset, pageSize, null);
      }

      for (Map<String, Object> row : rows) {
        row.put("_page", page);
        row.put("_totalRows", totalRows);
        row.put("_totalPages", totalPages);
        row.put("_reportName", reportName);
      }

      response.setData(rows);
    } catch (Exception e) {
      TraceBackService.trace(response, e);
    }
  }

  private String getParquetFile(String reportName) {
    return "s3://lake-analytics/" + reportName + ".parquet";
  }

  private long countExternalReportRows(
      String reportName, Map<String, Object> filters, String parquetFileOverride)
      throws SQLException {
    ExternalQuery externalQuery = buildExternalQuery(reportName, filters, parquetFileOverride);
    try (Connection connection =
            DriverManager.getConnection(
                getPgDuckDbJdbcUrl(), getPgDuckDbUsername(), getPgDuckDbPassword());
        PreparedStatement preparedStatement =
            connection.prepareStatement("SELECT COUNT(*)" + externalQuery.fromClause())) {
      bindPreparedStatement(preparedStatement, externalQuery.parameters(), 1);
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        return resultSet.next() ? resultSet.getLong(1) : 0L;
      }
    }
  }

  private List<Map<String, Object>> executeExternalReportQuery(
      String reportName,
      Map<String, Object> filters,
      int offset,
      int pageSize,
      String parquetFileOverride)
      throws SQLException {
    ExternalQuery externalQuery = buildExternalQuery(reportName, filters, parquetFileOverride);
    String sql =
        getExternalSelectClause(reportName)
            + externalQuery.fromClause()
            + getExternalOrderByClause(reportName)
            + " LIMIT ? OFFSET ?";

    try (Connection connection =
            DriverManager.getConnection(
                getPgDuckDbJdbcUrl(), getPgDuckDbUsername(), getPgDuckDbPassword());
        PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
      int nextIndex = bindPreparedStatement(preparedStatement, externalQuery.parameters(), 1);
      preparedStatement.setInt(nextIndex++, pageSize);
      preparedStatement.setInt(nextIndex, offset);
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        return mapResultSetRows(resultSet);
      }
    }
  }

  private ExternalQuery buildExternalQuery(
      String reportName, Map<String, Object> filters, String parquetFileOverride) {
    List<String> clauses = new ArrayList<>();
    List<Object> parameters = new ArrayList<>();
    clauses.add("1 = 1");

    boolean hasDept =
        "employee_manager_summary".equals(reportName)
            || "employee_department_salary_summary".equals(reportName);
    boolean hasRole =
        "employee_role_summary".equals(reportName) || "employee_manager_summary".equals(reportName);

    if (hasDept) {
      addPreparedInClause(
          clauses,
          parameters,
          getExternalColumnExpression("department"),
          getStringList(filters, "department"));
    }
    if (hasRole) {
      addPreparedInClause(
          clauses, parameters, getExternalColumnExpression("role"), getStringList(filters, "role"));
    }

    String parquetFile =
        parquetFileOverride == null || parquetFileOverride.isBlank()
            ? getParquetFile(reportName)
            : parquetFileOverride;
    String fromClause =
        " FROM duckdb.query('SELECT * FROM read_parquet(''"
            + escapeSqlLiteral(parquetFile)
            + "'')') e WHERE "
            + String.join(" AND ", clauses);
    return new ExternalQuery(fromClause, parameters);
  }

  private void addPreparedInClause(
      List<String> clauses, List<Object> parameters, String columnExpression, List<String> values) {
    if (values.isEmpty()) {
      return;
    }

    List<String> placeholders = new ArrayList<>();
    for (String value : values) {
      placeholders.add("?");
      parameters.add(value);
    }
    clauses.add(columnExpression + " IN (" + String.join(", ", placeholders) + ")");
  }

  private int bindPreparedStatement(
      PreparedStatement preparedStatement, List<Object> parameters, int startIndex)
      throws SQLException {
    int index = startIndex;
    for (Object parameter : parameters) {
      preparedStatement.setObject(index++, parameter);
    }
    return index;
  }

  private List<Map<String, Object>> mapResultSetRows(ResultSet resultSet) throws SQLException {
    ResultSetMetaData metaData = resultSet.getMetaData();
    int columnCount = metaData.getColumnCount();
    List<Map<String, Object>> rows = new ArrayList<>();

    while (resultSet.next()) {
      Map<String, Object> row = new LinkedHashMap<>();
      for (int i = 1; i <= columnCount; i++) {
        row.put(metaData.getColumnLabel(i), resultSet.getObject(i));
      }
      rows.add(row);
    }

    return rows;
  }

  private Double getNullableDouble(ResultSet resultSet, String columnName) throws SQLException {
    double value = resultSet.getDouble(columnName);
    return resultSet.wasNull() ? null : value;
  }

  private String getExternalSelectClause(String reportName) {
    if ("customer_predictions".equals(reportName)) {
      return "SELECT "
          + getExternalColumnExpression("account_id")
          + " AS account_id, "
          + getExternalColumnExpression("site_id")
          + " AS site_id, "
          + getExternalColumnExpression("customer_name")
          + " AS customer_name, "
          + getExternalColumnExpression("state")
          + " AS state, "
          + getExternalColumnExpression("contract_status")
          + " AS contract_status, "
          + getExternalColumnExpression("plan_type")
          + " AS plan_type, "
          + getExternalColumnExpression("current_rmr")
          + " AS current_rmr, "
          + getExternalColumnExpression("churn_risk_percentage")
          + " AS churn_risk_percentage, "
          + getExternalColumnExpression("risk_segment")
          + " AS risk_segment";
    }
    if ("employee_role_summary".equals(reportName)) {
      return "SELECT "
          + getExternalColumnExpression("role")
          + " AS role, "
          + getExternalColumnExpression("employee_count")
          + " AS employee_count, "
          + getExternalColumnExpression("average_salary")
          + " AS average_salary";
    }
    if ("employee_manager_summary".equals(reportName)) {
      return "SELECT "
          + getExternalColumnExpression("department")
          + " AS department, "
          + getExternalColumnExpression("role")
          + " AS role, "
          + getExternalColumnExpression("manager_name")
          + " AS manager_name, "
          + getExternalColumnExpression("dept_manager_count")
          + " AS dept_manager_count";
    }
    return "SELECT "
        + getExternalColumnExpression("department")
        + " AS department, "
        + getExternalColumnExpression("salary_band")
        + " AS salary_band, "
        + getExternalColumnExpression("employee_count")
        + " AS employee_count, "
        + getExternalColumnExpression("total_salary")
        + " AS total_salary";
  }

  private String getExternalOrderByClause(String reportName) {
    if ("customer_predictions".equals(reportName)) {
      return " ORDER BY account_id";
    }
    if ("employee_role_summary".equals(reportName)) {
      return " ORDER BY role";
    }
    return " ORDER BY department";
  }

  private String getExternalColumnExpression(String columnName) {
    if (List.of("churn_risk", "monthly_charges", "current_rmr", "churn_risk_percentage")
        .contains(columnName)) {
      return "CAST(e['" + columnName + "'] AS numeric)";
    }
    if ("age".equals(columnName)) {
      return "CAST(e['" + columnName + "'] AS integer)";
    }
    if (List.of("employee_count", "dept_manager_count").contains(columnName)) {
      return "CAST(e['" + columnName + "'] AS bigint)";
    }
    if (List.of("average_salary", "total_salary").contains(columnName)) {
      return "CAST(e['" + columnName + "'] AS numeric)";
    }
    return "CAST(e['" + columnName + "'] AS text)";
  }

  private String escapeSqlLiteral(String value) {
    return value.replace("'", "''");
  }

  private void validateCustomerCsv(Path csvPath) throws IOException {
    try (BufferedReader reader = Files.newBufferedReader(csvPath, StandardCharsets.UTF_8)) {
      String headerLine = reader.readLine();
      if (headerLine == null || headerLine.isBlank()) {
        throw new IllegalArgumentException("Customer CSV file is empty.");
      }

      String[] columns = headerLine.split(",");
      Set<String> normalizedColumns = new HashSet<>();
      for (String column : columns) {
        normalizedColumns.add(column.trim().toLowerCase(Locale.ROOT));
      }

      if (!normalizedColumns.containsAll(CUSTOMER_SCHEMA_COLUMNS)) {
        throw new IllegalArgumentException(
            "Customer CSV must contain columns: account_id, site_id, customer_first_name, customer_last_name, address_line1, address_line_2, city, state, country, contract_status, tenure_in_months, days_left_in_recent_contract, plan_type, current_rmr, billing_frequency, payment_mode, property_type, fico_score, customer_initial_contract_term, remaining_life_time, acv_segment, projected_rmr_churn, acv, cost_to_serve, rmr_difference_from_company_avg, area_attrition.");
      }
    }
  }

  private Path normalizeCustomerCsv(Path csvPath) throws IOException {
    String headerLine;
    try (BufferedReader reader = Files.newBufferedReader(csvPath, StandardCharsets.UTF_8)) {
      headerLine = reader.readLine();
    }

    if (headerLine == null || headerLine.isBlank()) {
      throw new IllegalArgumentException("Customer CSV file is empty.");
    }

    String[] columns = headerLine.split(",", -1);
    List<String> normalizedColumns = new ArrayList<>(columns.length);
    boolean changed = false;
    for (String column : columns) {
      String trimmed = column.trim();
      String replacement =
          CUSTOMER_SCHEMA_ALIASES.getOrDefault(trimmed.toLowerCase(Locale.ROOT), trimmed);
      normalizedColumns.add(replacement);
      if (!replacement.equals(trimmed)) {
        changed = true;
      }
    }

    if (!changed) {
      return csvPath;
    }

    Path normalizedCsvPath = Files.createTempFile("customer-profile-upload-", ".csv");
    List<String> lines = Files.readAllLines(csvPath, StandardCharsets.UTF_8);
    if (lines.isEmpty()) {
      throw new IllegalArgumentException("Customer CSV file is empty.");
    }
    lines.set(0, String.join(",", normalizedColumns));
    Files.write(normalizedCsvPath, lines, StandardCharsets.UTF_8);
    return normalizedCsvPath;
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

  private record ExternalQuery(String fromClause, List<Object> parameters) {}

  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> executeReportQuery(
      String reportName, String baseFrom, int offset, int pageSize, Map<String, Object> filters) {
    List<Map<String, Object>> rows = new ArrayList<>();
    if ("employee_role_summary".equals(reportName)) {
      Query query =
          createQuery(
              "SELECT e.role, e.employee_count, e.average_salary "
                  + baseFrom
                  + " ORDER BY e.role OFFSET "
                  + offset
                  + " LIMIT "
                  + pageSize,
              filters);
      for (Object[] result : (List<Object[]>) query.getResultList()) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("role", result[0]);
        row.put("employee_count", result[1]);
        row.put("average_salary", result[2]);
        rows.add(row);
      }
    } else if ("employee_manager_summary".equals(reportName)) {
      Query query =
          createQuery(
              "SELECT e.department, e.role, e.manager_name, e.dept_manager_count "
                  + baseFrom
                  + " ORDER BY e.department OFFSET "
                  + offset
                  + " LIMIT "
                  + pageSize,
              filters);
      for (Object[] result : (List<Object[]>) query.getResultList()) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("department", result[0]);
        row.put("role", result[1]);
        row.put("manager_name", result[2]);
        row.put("dept_manager_count", result[3]);
        rows.add(row);
      }
    } else if ("employee_department_salary_summary".equals(reportName)) {
      Query query =
          createQuery(
              "SELECT e.department, e.salary_band, e.employee_count, e.total_salary "
                  + baseFrom
                  + " ORDER BY e.department OFFSET "
                  + offset
                  + " LIMIT "
                  + pageSize,
              filters);
      for (Object[] result : (List<Object[]>) query.getResultList()) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("department", result[0]);
        row.put("salary_band", result[1]);
        row.put("employee_count", result[2]);
        row.put("total_salary", result[3]);
        rows.add(row);
      }
    } else {
      Query query =
          createQuery(
              "SELECT e.employee_id, e.name, e.department, e.role, e.age, e.salary, e.status "
                  + baseFrom
                  + " ORDER BY e.employee_id OFFSET "
                  + offset
                  + " LIMIT "
                  + pageSize,
              filters);
      for (Object[] result : (List<Object[]>) query.getResultList()) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("employeeId", result[0]);
        row.put("name", result[1]);
        row.put("department", result[2]);
        row.put("role", result[3]);
        row.put("age", result[4]);
        row.put("salary", result[5]);
        row.put("status", result[6]);
        rows.add(row);
      }
    }
    return rows;
  }

  private Map<String, Object> extractFilters(ActionRequest request) {
    Map<String, Object> filters = new LinkedHashMap<>();
    collectFilterValues(filters, request.getContext(), new HashSet<>());
    collectFilterValues(filters, request.getData(), new HashSet<>());
    return filters;
  }

  private void collectFilterValues(
      Map<String, Object> filters, Object source, Set<Object> visitedSources) {
    if (!(source instanceof Map<?, ?> sourceMap) || visitedSources.contains(source)) {
      return;
    }

    visitedSources.add(source);

    for (Map.Entry<String, List<String>> entry : MULTI_FILTER_ALIASES.entrySet()) {
      for (String alias : entry.getValue()) {
        if (sourceMap.containsKey(alias)) {
          mergeStringList(filters, entry.getKey(), extractStringList(sourceMap.get(alias)));
        }
      }
    }

    for (Map.Entry<String, List<String>> entry : SCALAR_FILTER_ALIASES.entrySet()) {
      if (filters.containsKey(entry.getKey())) {
        continue;
      }
      for (String alias : entry.getValue()) {
        if (sourceMap.containsKey(alias)) {
          Object scalarValue = extractScalarValue(sourceMap.get(alias));
          if (scalarValue != null && !String.valueOf(scalarValue).isBlank()) {
            filters.put(entry.getKey(), scalarValue);
            break;
          }
        }
      }
    }

    for (Object value : sourceMap.values()) {
      if (value instanceof Map<?, ?>) {
        collectFilterValues(filters, value, visitedSources);
      } else if (value instanceof Collection<?> collection) {
        for (Object item : collection) {
          if (item instanceof Map<?, ?>) {
            collectFilterValues(filters, item, visitedSources);
          }
        }
      }
    }
  }

  private void mergeStringList(Map<String, Object> filters, String key, List<String> values) {
    if (values.isEmpty()) {
      return;
    }

    @SuppressWarnings("unchecked")
    List<String> existing =
        (List<String>) filters.computeIfAbsent(key, ignored -> new ArrayList<>());
    for (String value : values) {
      if (!existing.contains(value)) {
        existing.add(value);
      }
    }
  }

  private List<String> extractStringList(Object value) {
    List<String> values = new ArrayList<>();
    collectStringValues(values, value);
    return values;
  }

  private void collectStringValues(List<String> values, Object value) {
    if (value == null) {
      return;
    }
    if (value instanceof Collection<?> collection) {
      for (Object item : collection) {
        collectStringValues(values, item);
      }
      return;
    }
    if (value instanceof Map<?, ?> valueMap) {
      for (String nestedKey : List.of("value", "name", "code", "fullName", "id")) {
        if (valueMap.containsKey(nestedKey)) {
          collectStringValues(values, valueMap.get(nestedKey));
          return;
        }
      }
      return;
    }

    String text = String.valueOf(value).trim();
    if (text.isEmpty()) {
      return;
    }
    if (text.contains(",")) {
      for (String token : text.split(",")) {
        collectStringValues(values, token);
      }
      return;
    }
    if (!values.contains(text)) {
      values.add(text);
    }
  }

  private Object extractScalarValue(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Collection<?> collection) {
      for (Object item : collection) {
        Object nestedValue = extractScalarValue(item);
        if (nestedValue != null) {
          return nestedValue;
        }
      }
      return null;
    }
    if (value instanceof Map<?, ?> valueMap) {
      for (String nestedKey : List.of("id", "value", "name", "code", "fullName")) {
        if (valueMap.containsKey(nestedKey)) {
          Object nestedValue = extractScalarValue(valueMap.get(nestedKey));
          if (nestedValue != null) {
            return nestedValue;
          }
        }
      }
      return null;
    }

    String text = String.valueOf(value).trim();
    return text.isEmpty() ? null : text;
  }

  private Map<String, Object> buildProfileRow(
      Map<String, Object> filters,
      String whereClause,
      String columnName,
      String dataType,
      String columnExpression,
      boolean checkDuplicates) {
    Map<String, Object> row = new LinkedHashMap<>();
    String baseFrom = " FROM lake_lake_employee e WHERE " + whereClause;
    String nullPredicate =
        isTextColumn(columnName)
            ? "(" + columnExpression + " IS NULL OR BTRIM(" + columnExpression + ") = '')"
            : columnExpression + " IS NULL";

    long totalCount = getLongValue("SELECT COUNT(*)" + baseFrom, filters);
    long nullCount = getLongValue("SELECT COUNT(*)" + baseFrom + " AND " + nullPredicate, filters);
    long distinctCount =
        getLongValue("SELECT COUNT(DISTINCT " + columnExpression + ")" + baseFrom, filters);
    String sampleValues = getSampleValues(filters, whereClause, columnName, columnExpression);

    String qualityNote = "Looks clean";
    if (checkDuplicates) {
      long duplicateCount =
          getLongValue(
              "SELECT COUNT(*) FROM (SELECT "
                  + columnExpression
                  + baseFrom
                  + " AND "
                  + columnExpression
                  + " IS NOT NULL GROUP BY "
                  + columnExpression
                  + " HAVING COUNT(*) > 1) duplicates",
              filters);
      if (duplicateCount > 0) {
        qualityNote = "Contains duplicate values";
      } else if (nullCount > 0) {
        qualityNote = "Contains null or blank values";
      }
    } else if (nullCount > 0) {
      qualityNote = "Contains null or blank values";
    }

    row.put("column_name", columnName);
    row.put("data_type", dataType);
    row.put("null_count", String.valueOf(nullCount));
    row.put("null_percentage", formatPercentage(nullCount, totalCount));
    row.put("distinct_count", String.valueOf(distinctCount));
    row.put("sample_values", sampleValues);
    row.put("quality_note", qualityNote);
    return row;
  }

  private String getSampleValues(
      Map<String, Object> filters, String whereClause, String columnName, String columnExpression) {
    String sampleValueExpression = columnExpression;
    String orderExpression = columnExpression;

    if ("salary".equals(columnName)) {
      sampleValueExpression = "TO_CHAR(" + columnExpression + ", 'FM999999999.00')";
    } else if ("age".equals(columnName)) {
      sampleValueExpression = "CAST(" + columnExpression + " AS text)";
    }

    String sql =
        "SELECT COALESCE(STRING_AGG(sample.sample_value, ', ' ORDER BY sample.sort_value), '') "
            + "FROM (SELECT DISTINCT "
            + sampleValueExpression
            + " AS sample_value, "
            + orderExpression
            + " AS sort_value FROM lake_lake_employee e WHERE "
            + whereClause
            + " AND "
            + columnExpression
            + " IS NOT NULL"
            + (isTextColumn(columnName) ? " AND BTRIM(" + columnExpression + ") <> ''" : "")
            + " ORDER BY "
            + orderExpression
            + " LIMIT 5) sample";

    return getStringValue(sql, filters);
  }

  private boolean isTextColumn(String columnName) {
    return !"age".equals(columnName) && !"salary".equals(columnName);
  }

  private String buildWhereClause(String reportName, Map<String, Object> filters) {
    List<String> clauses = new ArrayList<>();
    clauses.add("1 = 1");

    boolean hasDept =
        "dim_employee".equals(reportName)
            || "employee_manager_summary".equals(reportName)
            || "employee_department_salary_summary".equals(reportName);
    boolean hasRole =
        "dim_employee".equals(reportName)
            || "employee_role_summary".equals(reportName)
            || "employee_manager_summary".equals(reportName);
    boolean hasStatus = "dim_employee".equals(reportName);
    boolean hasAge = "dim_employee".equals(reportName);
    boolean hasSalary = "dim_employee".equals(reportName);

    if (hasDept)
      addInClause(clauses, "e.department", "department", getStringList(filters, "department"));
    if (hasRole) addInClause(clauses, "e.role", "role", getStringList(filters, "role"));
    if (hasStatus) addInClause(clauses, "e.status", "status", getStringList(filters, "status"));

    if (hasAge && getInteger(filters, "ageMin") != null) clauses.add("e.age >= :ageMin");
    if (hasAge && getInteger(filters, "ageMax") != null) clauses.add("e.age <= :ageMax");
    if (hasSalary && getDecimal(filters, "salaryMin") != null)
      clauses.add("e.salary >= :salaryMin");
    if (hasSalary && getDecimal(filters, "salaryMax") != null)
      clauses.add("e.salary <= :salaryMax");

    return String.join(" AND ", clauses);
  }

  private void addInClause(
      List<String> clauses, String columnExpression, String parameterPrefix, List<String> values) {
    if (values.isEmpty()) {
      return;
    }

    List<String> placeholders = new ArrayList<>();
    for (int i = 0; i < values.size(); i++) {
      placeholders.add(":" + parameterPrefix + i);
    }
    clauses.add(columnExpression + " IN (" + String.join(", ", placeholders) + ")");
  }

  private long getLongValue(String sql, Map<String, Object> filters) {
    Object value = createQuery(sql, filters).getSingleResult();
    return value == null ? 0L : ((Number) value).longValue();
  }

  private String getStringValue(String sql, Map<String, Object> filters) {
    Object value = createQuery(sql, filters).getSingleResult();
    return value == null ? "" : String.valueOf(value);
  }

  private Query createQuery(String sql, Map<String, Object> filters) {
    Query query = JPA.em().createNativeQuery(sql);

    bindStringList(query, "department", getStringList(filters, "department"));
    bindStringList(query, "role", getStringList(filters, "role"));
    bindStringList(query, "status", getStringList(filters, "status"));

    Integer ageMin = getInteger(filters, "ageMin");
    Integer ageMax = getInteger(filters, "ageMax");
    BigDecimal salaryMin = getDecimal(filters, "salaryMin");
    BigDecimal salaryMax = getDecimal(filters, "salaryMax");

    if (ageMin != null) {
      query.setParameter("ageMin", ageMin);
    }
    if (ageMax != null) {
      query.setParameter("ageMax", ageMax);
    }
    if (salaryMin != null) {
      query.setParameter("salaryMin", salaryMin);
    }
    if (salaryMax != null) {
      query.setParameter("salaryMax", salaryMax);
    }

    return query;
  }

  private void bindStringList(Query query, String prefix, List<String> values) {
    for (int i = 0; i < values.size(); i++) {
      query.setParameter(prefix + i, values.get(i));
    }
  }

  @SuppressWarnings("unchecked")
  private List<String> getStringList(Map<String, Object> filters, String key) {
    Object value = filters.get(key);
    if (value == null) {
      return List.of();
    }
    if (value instanceof List<?>) {
      return (List<String>) value;
    }
    return List.of(String.valueOf(value));
  }

  private String formatPercentage(long part, long total) {
    if (total <= 0) {
      return "0.00";
    }
    BigDecimal value =
        BigDecimal.valueOf(part)
            .multiply(BigDecimal.valueOf(100))
            .divide(BigDecimal.valueOf(total), 2, RoundingMode.HALF_UP);
    return value.toPlainString();
  }

  private Integer getInteger(Map<String, Object> filters, String key) {
    Object value = filters.get(key);
    if (value == null || String.valueOf(value).trim().isEmpty()) {
      return null;
    }
    if (value instanceof Number number) {
      return number.intValue();
    }
    return Integer.valueOf(String.valueOf(value).trim());
  }

  private BigDecimal getDecimal(Map<String, Object> filters, String key) {
    Object value = filters.get(key);
    if (value == null || String.valueOf(value).trim().isEmpty()) {
      return null;
    }
    if (value instanceof BigDecimal bigDecimal) {
      return bigDecimal;
    }
    if (value instanceof Number number) {
      return BigDecimal.valueOf(number.doubleValue());
    }
    return new BigDecimal(String.valueOf(value).trim());
  }

  private String runLakeAgent(String question) throws IOException, InterruptedException {
    Path projectRoot = Paths.get(System.getProperty("user.dir")).toAbsolutePath();
    Path agentDir = projectRoot.resolve("ai-agent");
    Path scriptPath = agentDir.resolve("ask.py");
    Path pythonPath = getLakeAgentPythonPath(agentDir);

    Process process =
        new ProcessBuilder(pythonPath.toString(), scriptPath.toString(), question)
            .directory(agentDir.toFile())
            .redirectErrorStream(true)
            .start();

    boolean completed = process.waitFor(AGENT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    if (!completed) {
      process.destroyForcibly();
      throw new IllegalStateException("AI agent timed out. Please try a smaller question.");
    }
    if (process.exitValue() != 0) {
      throw new IllegalStateException(normalizeAgentOutput(output));
    }
    return normalizeAgentOutput(output);
  }

  private String getAgentQuestion(Map<String, Object> context) {
    if (context == null) {
      return null;
    }

    for (String key : List.of("$agentQuestion", "agentQuestion")) {
      Object value = context.get(key);
      if (value != null && !String.valueOf(value).isBlank()) {
        return String.valueOf(value);
      }
    }
    return null;
  }

  private Path getLakeAgentPythonPath(Path agentDir) {
    String configuredPython = System.getenv("AXELOR_LAKE_AGENT_PYTHON");
    if (configuredPython != null && !configuredPython.isBlank()) {
      return Paths.get(configuredPython.trim()).toAbsolutePath();
    }

    Path windowsPython = agentDir.resolve(".venv").resolve("Scripts").resolve("python.exe");
    if (Files.exists(windowsPython)) {
      return windowsPython;
    }
    return agentDir.resolve(".venv").resolve("bin").resolve("python");
  }

  private String normalizeAgentOutput(String output) {
    String normalized = output == null ? "" : output.trim();
    return normalized.isBlank() ? "AI agent returned no response." : normalized;
  }

  private long toLong(Object value) {
    return value == null ? 0L : ((Number) value).longValue();
  }

  private Double toDouble(Object value) {
    return value == null ? null : ((Number) value).doubleValue();
  }
}
