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

import com.axelor.lake.db.LakehouseTable;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface LakehouseService {

  int DEFAULT_PREVIEW_LIMIT = 100;

  LakehouseTable uploadToLakehouse(File csv, String tableName)
      throws IOException, InterruptedException, SQLException;

  LakehouseTable uploadToLakehouse(File csv, String tableName, boolean triggerPipeline)
      throws IOException, InterruptedException, SQLException;

  List<Map<String, Object>> queryFromLakehouse(String tableName)
      throws IOException, InterruptedException, SQLException;

  List<Map<String, Object>> queryFromLakehouse(String tableName, int limit)
      throws IOException, InterruptedException, SQLException;

  List<Map<String, Object>> queryFromLakehouseByMetadataPath(String metadataPath, int limit)
      throws SQLException;

  void prepareProfilingData(String tableName, String metadataPath) throws SQLException;

  void prepareProfilingDataAsync(String tableName, String metadataPath) throws SQLException;

  String getLatestMetadataPath(String tableName) throws IOException, InterruptedException;

  void deleteTable(String tableName) throws IOException, InterruptedException;

  void trainTelecomModel(String tableName) throws IOException, InterruptedException;

  void predictTelecomData(String tableName) throws IOException, InterruptedException;

  void runCustomerRiskWorkflow(String tableName) throws IOException, InterruptedException;
}
