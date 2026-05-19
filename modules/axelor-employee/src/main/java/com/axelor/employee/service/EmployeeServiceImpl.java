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
package com.axelor.employee.service;

import com.axelor.apps.base.db.Localization;
import com.axelor.apps.base.db.repo.LocalizationRepository;
import com.axelor.auth.AuthService;
import com.axelor.auth.db.Group;
import com.axelor.auth.db.User;
import com.axelor.auth.db.repo.GroupRepository;
import com.axelor.auth.db.repo.UserRepository;
import com.axelor.db.Query;
import com.axelor.employee.db.Employee;
import com.axelor.employee.db.EmployeeRole;
import jakarta.inject.Inject;
import java.time.LocalDateTime;

public class EmployeeServiceImpl implements EmployeeService {

  protected static final String DEFAULT_LOCALIZATION_CODE = "en_GB";

  @Inject private UserRepository userRepo;
  @Inject private GroupRepository groupRepo;
  @Inject private LocalizationRepository localizationRepo;
  @Inject private AuthService authService;

  @Override
  public void generateEmail(Employee employee) {

    if (employee.getFirstName() != null && employee.getLastName() != null) {

      String firstName = employee.getFirstName().trim().toLowerCase();
      String lastName = employee.getLastName().trim().toLowerCase();

      String email = firstName.substring(0, 1) + lastName + "@voziq.ai";

      employee.setEmail(email);
    }
  }

  @Override
  public void generateEmployeeId(Employee employee) {

    if (employee.getEmployeeId() == null) {

      long count = Query.of(Employee.class).count() + 1;

      String empId = String.format("EMP%03d", count);

      employee.setEmployeeId(empId);
    }
  }

  @Override
  public void prepareEmployee(Employee employee) {
    generateEmployeeId(employee);
    generateEmail(employee);
  }

  @Override
  public User createUserForEmployee(Employee employee) {

    if (employee == null || employee.getEmployeeId() == null) {
      return null;
    }

    User user = userRepo.findByCode(employee.getEmployeeId());
    boolean isNewUser = user == null;

    if (isNewUser) {
      user = new User();
      user.setCode(employee.getEmployeeId());
      user.setPassword(authService.encrypt("voziq"));
    }

    user.setName(buildEmployeeName(employee));
    user.setEmail(employee.getEmail());
    user.setGroup(findGroup(employee.getRole()));
    user.setLocalization(findLocalization());
    user.setLanguage("en");
    user.setBlocked(false);

    user.setActivateOn(LocalDateTime.now());

    return userRepo.save(user);
  }

  protected String buildEmployeeName(Employee employee) {
    String firstName = employee.getFirstName() == null ? "" : employee.getFirstName().trim();
    String lastName = employee.getLastName() == null ? "" : employee.getLastName().trim();
    return (firstName + " " + lastName).trim();
  }

  protected Group findGroup(EmployeeRole employeeRole) {
    if (employeeRole == null || employeeRole.getRole() == null) {
      return null;
    }

    String roleName = employeeRole.getRole().trim();
    Group group = groupRepo.findByCode(roleName);

    if (group == null) {
      group = groupRepo.findByName(roleName);
    }

    return group;
  }

  protected Localization findLocalization() {
    Localization localization = localizationRepo.findByCode(DEFAULT_LOCALIZATION_CODE);

    if (localization == null) {
      localization = localizationRepo.findByCode("en_US");
    }

    return localization;
  }
}
