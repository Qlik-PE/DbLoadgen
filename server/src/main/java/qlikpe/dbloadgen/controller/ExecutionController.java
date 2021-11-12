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
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;
import qlikpe.dbloadgen.DbLoadgenProperties;
import qlikpe.dbloadgen.model.database.Database;
import qlikpe.dbloadgen.model.output.OutputBuffer;
import qlikpe.dbloadgen.model.output.OutputBufferMap;
import qlikpe.dbloadgen.model.workload.*;
import qlikpe.dbloadgen.service.BuildInfo;
import qlikpe.dbloadgen.service.WorkloadService;

import java.util.List;
import java.util.Map;
import java.util.Properties;

@Controller
public final class ExecutionController {
  private static final Logger LOG = LogManager.getLogger(ExecutionController.class);
  private final ApplicationContext context;

  private final DbLoadgenProperties dbLoadgenProperties;
  private final WorkloadConfigList workloadConfigList;
  private final WorkloadService workloadService;

  @Autowired
  public ExecutionController(ApplicationContext context,
                             WorkloadService workloadService) {
    dbLoadgenProperties = DbLoadgenProperties.getInstance();
    workloadConfigList = WorkloadConfigList.getInstance();
    this.workloadService = workloadService;
    this.context = context;
  }

  @RequestMapping(value = "/execute-command", method = RequestMethod.POST)
  public String executeCommand(Model model, @RequestParam Map<String,String> allParams) {
    String connectionName = allParams.get("connectionName");
    String workloadName = allParams.get("workloadName");
    String connectionUrl = allParams.get("connectionUrl");
    String userName = allParams.get("userName");
    String password = allParams.get("password");
    String connectionPrefix = DbLoadgenProperties.CONNECTION_PREFIX + connectionName + ".";
    dbLoadgenProperties.setProperty(DbLoadgenProperties.CONNECTION_NAME, connectionName);
    dbLoadgenProperties.setProperty(connectionPrefix + "databaseType", allParams.get("databaseType"));
    dbLoadgenProperties.setProperty(connectionPrefix + "jdbcDriver", allParams.get("jdbcDriver"));
    dbLoadgenProperties.setProperty(connectionPrefix + "url", connectionUrl);
    dbLoadgenProperties.setProperty(connectionPrefix + "userName", userName);
    dbLoadgenProperties.setProperty(connectionPrefix + "password", password);
    dbLoadgenProperties.setProperty(DbLoadgenProperties.WORKLOAD_CONFIG_FILE, workloadName);
    workloadService.setProperties(dbLoadgenProperties.getProperties());

    // update values in the connection information so that we can retrieve it again
    // when we return to the home page.
    DbLoadgenConnectionInfo connection = dbLoadgenProperties.getConnectionList().getConnection(connectionName);
    connection.setUrl(connectionUrl);
    connection.setUsername(userName);
    connection.setPassword(password);

    WorkloadConfig workloadConfig = workloadConfigList.getWorkloadConfigByName(workloadName);
    workloadConfig.setSchema(allParams.get("schema"));
    workloadConfig.setPreloadThreads(Integer.parseInt(allParams.get("preloadThreads")));
    workloadConfig.setPreloadBatchSize(Integer.parseInt(allParams.get("preloadBatchSize")));
    workloadConfig.setDuration(Integer.parseInt(allParams.get("duration")));
    workloadConfig.setWorkerThreads(Integer.parseInt(allParams.get("workerThreads")));
    workloadConfig.setWorkerThreadSleep(Integer.parseInt(allParams.get("workerThreadSleep")));
    workloadConfig.setWorkerBatchSize(Integer.parseInt(allParams.get("workerBatchSize")));
    workloadConfig.setWorkerBatchSleep(Integer.parseInt(allParams.get("workerBatchSleep")));
    workloadConfig.setAllTablesAllThreads(allParams.get("allTablesAllThreads"));
    workloadConfig.setOperationPct(new OperationPct(Integer.parseInt(allParams.get("insertPct")),
            Integer.parseInt(allParams.get("updatePct")),
            Integer.parseInt(allParams.get("deletePct"))));
    String command = allParams.get("command");

    /* dbLoadgenProperties.printProperties("execution controller", dbLoadgenProperties.getProperties()); */

    try {
      workloadService.executeCommand(command);
      workloadService.saveContext();
    } catch (WorkloadInitializationException e) {
      LOG.error("Failed to initialize workload: {} message: {}", workloadName, e.getMessage());
    }

    return "redirect:/execution-results";
  }

  @RequestMapping(value = "/execution-results", method = RequestMethod.GET)
  public String executionResults(Model model) {

    StringBuilder results, preloadResults, cdcResults;

    OutputBuffer preloadStats = OutputBufferMap.getInstance().getOutputBufferByName(OutputBufferMap.PRELOAD_STATS);
    if (preloadStats != null) {
      preloadResults = new StringBuilder();
      workloadService.refreshPreloadStats();
      buildResults(preloadResults, preloadStats);
      model.addAttribute("preloadResults", preloadResults.toString());
    }
    OutputBuffer runtimeStats = OutputBufferMap.getInstance().getOutputBufferByName(OutputBufferMap.RUNTIME_STATS);
    if (runtimeStats != null) {
      cdcResults = new StringBuilder();
      workloadService.refreshRuntimeStats();
      buildResults(cdcResults, runtimeStats);
      model.addAttribute("cdcResults", cdcResults.toString());
    }
    if (runtimeStats == null && preloadStats == null) {
      // only display this stuff if we aren't displaying table information.
      results = new StringBuilder();

      buildResults(results, OutputBufferMap.getInstance().getOutputBufferByName(OutputBufferMap.TEST_CONNECTION));
      buildResults(results, OutputBufferMap.getInstance().getOutputBufferByName(OutputBufferMap.CLEANUP));
      buildResults(results, OutputBufferMap.getInstance().getOutputBufferByName(OutputBufferMap.INITIALIZE_SCHEMA));
      model.addAttribute("results", results.toString());
    }
    if (workloadService.isPreloadRunning()) {
      model.addAttribute("preload",  "true");
      model.addAttribute("preloadPct", workloadService.getWorkloadManager().getPreloadPct());
    } else {
      model.addAttribute("preload",  "false");
    }
    if (workloadService.isCdcTestExecuting()) {
      model.addAttribute("cdcTest",  "true");
      model.addAttribute("cdcTestPct", workloadService.getWorkloadManager().getCdcElapsedPct());
    } else {
      model.addAttribute("cdcTest",  "false");
    }

    return "execution-results";
  }

  /**
   * The stop button was pressed. Send an interrupt to all preload
   * or worker threads.
   * @param model the model.
   * @return the home page.
   */
  @RequestMapping(value = "/stop-execution", method = RequestMethod.POST)
  public String stopExecution(Model model) {
    workloadService.stopThreads();
    return "redirect:/";
  }

  /**
   * Append an output buffer to the execution results if the buffer exists.
   * @param builder an instance of StringBuilder.
   * @param outputBuffer the OutputBuffer to work with.
   */
  private void buildResults(StringBuilder builder, OutputBuffer outputBuffer) {
    if (outputBuffer != null) {
      builder.append(outputBuffer.asHtml());
    }
  }

}
