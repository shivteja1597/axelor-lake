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
package com.axelor.employee.db.repo;

import com.axelor.employee.db.Employee;
import com.axelor.employee.service.EmployeeService;
import jakarta.inject.Inject;
import jakarta.persistence.PersistenceException;

public class EmployeeManagementRepository extends EmployeeRepository {

  protected EmployeeService employeeService;

  @Inject
  public EmployeeManagementRepository(EmployeeService employeeService) {
    this.employeeService = employeeService;
  }

  @Override
  public Employee save(Employee employee) {
    try {
      employeeService.prepareEmployee(employee);
      employee = super.save(employee);
      employeeService.createUserForEmployee(employee);
      return employee;
    } catch (Exception e) {
      throw new PersistenceException(e.getMessage(), e);
    }
  }
}
