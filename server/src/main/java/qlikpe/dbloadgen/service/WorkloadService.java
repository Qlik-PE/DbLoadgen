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
package qlikpe.dbloadgen.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import qlikpe.dbloadgen.DbLoadgenProperties;
import qlikpe.dbloadgen.model.workload.WorkloadInitializationException;
import qlikpe.dbloadgen.model.workload.WorkloadManager;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Exposes the common Workload management functionality as a
 * service for Spring Boot.
 */
@Service
public class WorkloadService {
    private static final Logger LOG = LogManager.getLogger(WorkloadService.class);
    private final WorkloadManager workloadManager;
    private final Environment env;
    private final Map<String, Object> applicationProperties;

    @Autowired
    public WorkloadService(Environment env) {
        LOG.info("WorkloadService constructor");
        workloadManager = new WorkloadManager();
        applicationProperties = new HashMap<>();
        this.env = env;

        setApplicationProperty(DbLoadgenProperties.DATADIR);
        setApplicationProperty(DbLoadgenProperties.WORKLOAD_CONFIG_FILE);
        setApplicationProperty(DbLoadgenProperties.CONNECTION_NAME);
        setApplicationProperty(DbLoadgenProperties.CONNECTION_LIST);

        try {
            Properties runtimeProperties = DbLoadgenProperties.getInstance().initProperties(applicationProperties);
            DbLoadgenProperties.getInstance().printProperties("WorkloadService() Constructor", runtimeProperties);
            setProperties(runtimeProperties);

        } catch(IOException e) {
            LOG.error("error initializing DbLoadgenProperties: {}", e.getMessage());
        }

    }

    /**
     * Check if an application property was set, and add it to the
     * list of properties from the environment.
     * @param key a key to look for.
     */
    private void setApplicationProperty(String key) {
        String value = env.getProperty(key);
        if (value != null) {
            LOG.info("setting application property: {} value: {}", key, value);
            applicationProperties.put(key, value);
        }
    }

    public WorkloadManager getWorkloadManager() { return workloadManager; }

    public void saveContext()  {
        workloadManager.saveContext();
    }

    /**
     * Set the properties for the workload.
     * @param props the properties we want to set.
     */
    public void setProperties(Properties props) {
        workloadManager.setPropertyManager(props);
    }

    /**
     * Execute the requested command against the workload
     * @param command the command to execute.
     * @throws WorkloadInitializationException on error.
     */
    public void executeCommand(String command) throws WorkloadInitializationException {
        workloadManager.executeCommand(command, true);
    }

    /**
     * Refresh the preload statistics so that we can output the latest.
     * We only refresh while the preload step is executing.
     */
    public void refreshPreloadStats() {
        if (isPreloadRunning())
            workloadManager.getPreloadStats();
    }

    /**
     * Signal any active threads to stop execution.
     */
    public void stopThreads() {
        workloadManager.stopThreads();
    }

    /**
     * Refresh the runtime statistics so that we can output the latest.
     * We will only refresh while the CDC test is executing.
     */
    public void refreshRuntimeStats() {
        if (isCdcTestExecuting())
            workloadManager.getRuntimeStats();
    }

    /**
     * Lets us know if the preload step is currently executing.
     * @return true if it is running, false otherwise.
     */
    public boolean isPreloadRunning() {
        return workloadManager.isPreloadRunning();
    }

    /**
     * Lets us know if the CDC test is currently executing.
     * @return true if it is running, false otherwise.
     */
    public boolean isCdcTestExecuting() {
        return workloadManager.isCdcTestRunning();
    }

    /**
     * Lets us know if we are currently parsing table metadata. This phase can
     * take awhile if there are a lot of tables or if the table metadata containns
     * actual data to use during preload.
     *
     * @return true if we are parsing metadata, false otherwise.
     */
    public boolean isParsingMetadata() { return workloadManager.isParsingMetadata(); }

    /**
     * Checks if the "stopThreads" flag has been set.
     * @return true if we are stopping (or stopped), false otherwise.
     */
    public boolean stopping() { return workloadManager.isStopThreads(); }

    /**
     * Lets us know if we are currently initializing the database schema.
     * @return true if we are, false otherwise.
     */
    public boolean isInitializingSchema() { return workloadManager.isInitializingSchema(); }

    public boolean executionInProgress() {
        return !stopping() &&
                (isPreloadRunning() || isCdcTestExecuting() || isParsingMetadata() || isInitializingSchema());
    }
}
