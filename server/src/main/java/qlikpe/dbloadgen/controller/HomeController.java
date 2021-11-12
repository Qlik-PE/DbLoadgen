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
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.info.BuildProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;
import qlikpe.dbloadgen.DbLoadgenProperties;
import qlikpe.dbloadgen.model.database.Database;
import qlikpe.dbloadgen.model.workload.*;
import qlikpe.dbloadgen.service.BuildInfo;
import qlikpe.dbloadgen.service.WorkloadService;

import java.util.List;
import java.util.Properties;

@Controller
public final class HomeController {
  private static final Logger LOG = LogManager.getLogger(HomeController.class);
  private final BuildProperties buildProperties;
  private final ApplicationContext context;

  private final DbLoadgenProperties dbLoadgenProperties;
  private final WorkloadConfigList workloadConfigList;
  private final WorkloadService workloadService;

  @Autowired
  public HomeController(ObjectProvider<BuildInfo> buildInfoProvider, ApplicationContext context,
                        WorkloadService workloadService) {
    dbLoadgenProperties = DbLoadgenProperties.getInstance();
    workloadConfigList = WorkloadConfigList.getInstance();
    this.workloadService = workloadService;
    this.buildProperties = buildInfoProvider.stream()
        .map(BuildInfo::getBuildProperties)
        .findAny()
        .orElseGet(HomeController::blankBuildProperties);
    this.context = context;


  }

  private static BuildProperties blankBuildProperties() {
    final var properties = new Properties();
    properties.setProperty("version", "unset");
    properties.setProperty("time", String.valueOf(System.currentTimeMillis()));
    return new BuildProperties(properties);
  }

  @RequestMapping("/")
  public String homePage(Model model,
                         @RequestParam(name="connectionName", required = false) String connectionRequest,
                         @RequestParam(name="workloadName", required = false) String workloadRequest) {


    if (connectionRequest != null) {
      // set the connection name to the choice from the selection box.
      LOG.info("connection request: {}", connectionRequest);
      dbLoadgenProperties.setProperty(DbLoadgenProperties.CONNECTION_NAME, connectionRequest);
    }
    if (workloadRequest != null) {
      // set the workload name to the choice from the selection box.
      LOG.info("Workload Request: {}", workloadRequest);
      dbLoadgenProperties.setProperty(DbLoadgenProperties.WORKLOAD_CONFIG_FILE, workloadRequest);
    }
    //workloadService.workloadInit();
    //WorkloadConfig workloadConfig = workloadService.getWorkloadManager().getWorkloadConfig();

    String workloadName = dbLoadgenProperties.getProperty(DbLoadgenProperties.WORKLOAD_CONFIG_FILE);
    WorkloadConfig workloadConfig = workloadConfigList.getWorkloadConfigByName(workloadName);
    String connectionName = dbLoadgenProperties.getProperty(DbLoadgenProperties.CONNECTION_NAME);
    DbLoadgenConnectionList connectionList = dbLoadgenProperties.getConnectionList();
    DbLoadgenConnectionInfo connection = connectionList.getConnection(connectionName);
    List<String> workloadNames = workloadConfigList.getWorkloadNames();
    List<String> connectionNames = connectionList.getConnectionNames();
    model.addAttribute("buildProperties", buildProperties);
    model.addAttribute("connectionNames", connectionNames);
    model.addAttribute("connection", connection);
    model.addAttribute("workloadNames", workloadNames);
    model.addAttribute("workload", workloadConfig);

    return "home-page";
  }

  @ResponseStatus(HttpStatus.OK)
  @RequestMapping("/health_check")
  public void healthCheck() {
  }

}
