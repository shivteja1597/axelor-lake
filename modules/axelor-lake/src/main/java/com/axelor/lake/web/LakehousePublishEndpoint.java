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

import com.axelor.inject.Beans;
import com.axelor.lake.service.LakehouseService;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.Map;

@Path("/lake/customer-predictions")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class LakehousePublishEndpoint {

  @POST
  @Path("/publish")
  public Response publishCustomerPredictions(@QueryParam("tableName") String tableName) {
    String resolvedTableName =
        tableName == null || tableName.isBlank() ? "customer_profile" : tableName.trim();
    try {
      int rowsLoaded =
          Beans.get(LakehouseService.class).publishCustomerPredictions(resolvedTableName);

      Map<String, Object> payload = new LinkedHashMap<>();
      payload.put("status", "ok");
      payload.put("tableName", resolvedTableName);
      payload.put("rowsLoaded", rowsLoaded);
      payload.put("message", "Customer predictions published to PostgreSQL.");
      return Response.ok(payload).build();
    } catch (Exception e) {
      Map<String, Object> payload = new LinkedHashMap<>();
      payload.put("status", "error");
      payload.put("tableName", resolvedTableName);
      payload.put("message", e.getMessage());
      return Response.serverError().entity(payload).build();
    }
  }
}
