/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package qlikpe.dbloadgen.controller;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;
import qlikpe.dbloadgen.config.DatabaseConfiguration;
import qlikpe.dbloadgen.config.WorkloadConfiguration;
import qlikpe.dbloadgen.model.database.Database;
import qlikpe.dbloadgen.model.database.MySqlDialect;
import qlikpe.dbloadgen.service.BuildInfo;

import org.springframework.beans.factory.*;
import org.springframework.boot.info.*;
import org.springframework.http.*;
import org.springframework.stereotype.*;
import org.springframework.ui.*;
import org.springframework.web.bind.annotation.*;

import javax.sql.DataSource;
import java.util.*;

@Controller
public final class HomeController {
  private static final Logger LOG = LogManager.getLogger(MySqlDialect.class);
  private final BuildProperties buildProperties;
  private final WorkloadConfiguration workload;
  private final DatabaseConfiguration databaseProperties;
  private final ApplicationContext context;

  public HomeController(ObjectProvider<BuildInfo> buildInfoProvider, WorkloadConfiguration workload,
                        DatabaseConfiguration databaseProperties, ApplicationContext context) {
    this.buildProperties = buildInfoProvider.stream()
        .map(BuildInfo::getBuildProperties)
        .findAny()
        .orElseGet(HomeController::blankBuildProperties);
    this.workload = workload;
    this.context = context;
    this.databaseProperties = databaseProperties;
  }

  private static BuildProperties blankBuildProperties() {
    final var properties = new Properties();
    properties.setProperty("version", "unset");
    properties.setProperty("time", String.valueOf(System.currentTimeMillis()));
    return new BuildProperties(properties);
  }

  @RequestMapping("/")
  public String homePage(Model model,
                        @ModelAttribute("message") String message) {
                        //@RequestParam(name="message", required = false, defaultValue = "no message") String message) {
    model.addAttribute("buildProperties", buildProperties);
    model.addAttribute("message", message);

    return "home-page";
  }

  @RequestMapping(value = "/test-connection", method = RequestMethod.POST)
  public String testConnection(Model model, RedirectAttributes redirectAttributes) {

    String message = "foobar";
    Database db;

    redirectAttributes.addFlashAttribute("message", message);
    return "redirect:/";
    //return "redirect:/" + "?message=" + message;
  }

  /**
   * This function needs to be called after the user has set the
   * jdbc properties to access the database. It will override the
   * default settings of the Spring datasource to use these
   * new settings.
   */
  public void refreshLoadgenJdbc(Database db) {
    DataSource ds = (DataSource) context.getBean("loadgenDataSource", db);
    JdbcTemplate loadgenJdbcTemplate = (JdbcTemplate) context.getBean("loadgenJdbcTemplate");
    loadgenJdbcTemplate.setDataSource(ds);
  }

  @ResponseStatus(HttpStatus.OK)
  @RequestMapping("/health_check")
  public void healthCheck() {
  }

}
