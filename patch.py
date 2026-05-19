import re

with open('/var/jenkins_home/project/modules/axelor-lake/src/main/java/com/axelor/lake/web/LakehouseController.java', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace SCALAR_FILTER_ALIASES
content = re.sub(
    r'private static final Map<String, List<String>> SCALAR_FILTER_ALIASES =.*?Map\.of\(',
    'private static final Map<String, List<String>> SCALAR_FILTER_ALIASES =\n      Map.of(\n          "analysisReport", List.of("$analysisReport", "analysisReport"),',
    content,
    flags=re.DOTALL
)

# Replace buildWhereClause
old_where = '''  private String buildWhereClause(Map<String, Object> filters) {
    List<String> clauses = new ArrayList<>();
    clauses.add("1 = 1");

    addInClause(clauses, "e.department", "department", getStringList(filters, "department"));
    addInClause(clauses, "e.role", "role", getStringList(filters, "role"));
    addInClause(clauses, "e.status", "status", getStringList(filters, "status"));

    if (getInteger(filters, "ageMin") != null) {
      clauses.add("e.age >= :ageMin");
    }
    if (getInteger(filters, "ageMax") != null) {
      clauses.add("e.age <= :ageMax");
    }
    if (getDecimal(filters, "salaryMin") != null) {
      clauses.add("e.salary >= :salaryMin");
    }
    if (getDecimal(filters, "salaryMax") != null) {
      clauses.add("e.salary <= :salaryMax");
    }

    return String.join(" AND ", clauses);
  }'''

new_where = '''  private String buildWhereClause(String reportName, Map<String, Object> filters) {
    List<String> clauses = new ArrayList<>();
    clauses.add("1 = 1");

    boolean hasDept = "dim_employee".equals(reportName) || "employee_manager_summary".equals(reportName) || "employee_department_salary_summary".equals(reportName);
    boolean hasRole = "dim_employee".equals(reportName) || "employee_role_summary".equals(reportName) || "employee_manager_summary".equals(reportName);
    boolean hasStatus = "dim_employee".equals(reportName);
    boolean hasAge = "dim_employee".equals(reportName);
    boolean hasSalary = "dim_employee".equals(reportName);

    if (hasDept) addInClause(clauses, "e.department", "department", getStringList(filters, "department"));
    if (hasRole) addInClause(clauses, "e.role", "role", getStringList(filters, "role"));
    if (hasStatus) addInClause(clauses, "e.status", "status", getStringList(filters, "status"));

    if (hasAge && getInteger(filters, "ageMin") != null) clauses.add("e.age >= :ageMin");
    if (hasAge && getInteger(filters, "ageMax") != null) clauses.add("e.age <= :ageMax");
    if (hasSalary && getDecimal(filters, "salaryMin") != null) clauses.add("e.salary >= :salaryMin");
    if (hasSalary && getDecimal(filters, "salaryMax") != null) clauses.add("e.salary <= :salaryMax");

    return String.join(" AND ", clauses);
  }'''
content = content.replace(old_where, new_where)

# Replace buildProfileRow
content = content.replace('" FROM lake_lake_employee e WHERE " + whereClause',
                          '" FROM duckdb.query(\'SELECT * FROM read_parquet(\\\'s3://lake-curated/dim_employee.parquet\\\')\') e WHERE " + whereClause')
content = content.replace('FROM (SELECT DISTINCT "\\n            + sampleValueExpression\\n            + " AS sample_value, "\\n            + orderExpression\\n            + " AS sort_value FROM lake_lake_employee e WHERE "',
                          'FROM (SELECT DISTINCT "\\n            + sampleValueExpression\\n            + " AS sample_value, "\\n            + orderExpression\\n            + " AS sort_value FROM duckdb.query(\'SELECT * FROM read_parquet(\\\'s3://lake-curated/dim_employee.parquet\\\')\') e WHERE "')

# Update loadColumnProfiles
content = content.replace('String whereClause = buildWhereClause(filters);', 'String whereClause = buildWhereClause("dim_employee", filters);', 1)

# Rip out loadFilteredEmployees and old pivot logic
new_load_filtered = '''  public void loadFilteredEmployees(ActionRequest request, ActionResponse response) {
    try {
      ensureProfilingDataAvailable();
      Map<String, Object> filters = extractFilters(request);
      String reportName = (String) filters.get("analysisReport");
      if (reportName == null || reportName.isBlank()) {
        reportName = "dim_employee";
      }

      String whereClause = buildWhereClause(reportName, filters);
      String baseFrom = " FROM duckdb.query('SELECT * FROM read_parquet(''" + getParquetFile(reportName) + "'')') e WHERE " + whereClause;

      int pageSize = normalizePageSize(getInteger(filters, "analysisPageSize"));
      long totalRows = getLongValue("SELECT COUNT(*)" + baseFrom, filters);
      int totalPages = totalRows <= 0 ? 1 : (int) Math.ceil((double) totalRows / pageSize);
      int page = normalizePage(getInteger(filters, "analysisPage"), totalPages);
      int offset = (page - 1) * pageSize;

      List<Map<String, Object>> rows = executeReportQuery(reportName, baseFrom, offset, pageSize, filters);

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
    if ("dim_employee".equals(reportName)) {
      return "s3://lake-curated/dim_employee.parquet";
    }
    return "s3://lake-analytics/" + reportName + ".parquet";
  }

  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> executeReportQuery(String reportName, String baseFrom, int offset, int pageSize, Map<String, Object> filters) {
    List<Map<String, Object>> rows = new ArrayList<>();
    if ("employee_role_summary".equals(reportName)) {
      Query query = createQuery("SELECT e.role, e.employee_count, e.average_salary " + baseFrom + " ORDER BY e.role OFFSET " + offset + " LIMIT " + pageSize, filters);
      for (Object[] result : (List<Object[]>) query.getResultList()) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("role", result[0]);
        row.put("employee_count", result[1]);
        row.put("average_salary", result[2]);
        rows.add(row);
      }
    } else if ("employee_manager_summary".equals(reportName)) {
      Query query = createQuery("SELECT e.department, e.role, e.manager_name, e.dept_manager_count " + baseFrom + " ORDER BY e.department OFFSET " + offset + " LIMIT " + pageSize, filters);
      for (Object[] result : (List<Object[]>) query.getResultList()) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("department", result[0]);
        row.put("role", result[1]);
        row.put("manager_name", result[2]);
        row.put("dept_manager_count", result[3]);
        rows.add(row);
      }
    } else if ("employee_department_salary_summary".equals(reportName)) {
      Query query = createQuery("SELECT e.department, e.salary_band, e.employee_count, e.total_salary " + baseFrom + " ORDER BY e.department OFFSET " + offset + " LIMIT " + pageSize, filters);
      for (Object[] result : (List<Object[]>) query.getResultList()) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("department", result[0]);
        row.put("salary_band", result[1]);
        row.put("employee_count", result[2]);
        row.put("total_salary", result[3]);
        rows.add(row);
      }
    } else {
      Query query = createQuery("SELECT e.employee_id, e.name, e.department, e.role, e.age, e.salary, e.status " + baseFrom + " ORDER BY e.employee_id OFFSET " + offset + " LIMIT " + pageSize, filters);
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
'''

start_idx = content.find('  public void loadFilteredEmployees(ActionRequest request, ActionResponse response) {')
end_idx = content.find('  private Map<String, Object> extractFilters(ActionRequest request) {\n')

content = content[:start_idx] + new_load_filtered + content[end_idx:]

with open('/var/jenkins_home/project/modules/axelor-lake/src/main/java/com/axelor/lake/web/LakehouseController.java', 'w', encoding='utf-8') as f:
    f.write(content)
