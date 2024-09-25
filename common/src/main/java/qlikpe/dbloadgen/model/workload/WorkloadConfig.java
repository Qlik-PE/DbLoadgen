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
package qlikpe.dbloadgen.model.workload;

import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Properties;

import static java.lang.Math.abs;

/**
 * The workload configuration that is parsed from the workload.yml file.
 * Note that this class is constructed by te yaml parser.
 */
@Getter
@Setter
public class WorkloadConfig {
    private static final int DEFAULT_DURATION = 10;  // ten minutes
    private static final int ONE_DAY = 1440;
    private static final Logger LOG = LogManager.getLogger(WorkloadConfig.class);

    private String name;
    private String dataset;
    private String schema;
    private OperationPct operationPct;
    private int workerThreads;
    private int preloadThreads;
    private int duration;
    private int preloadBatchSize;
    private int workerBatchSize;
    private int workerBatchSleep;
    private int workerThreadSleep;
    private String allTablesAllThreads;
    private List<TableConfig> tables;

    public void setDuration (int duration) {
        String message;
        if (duration > 0) {
            this.duration = duration;
            message = String.format("%s: workload duration set to %d minutes", this.name, duration);

        }
        else if (duration < 0) {
            this.duration = abs(duration) * ONE_DAY;
            message = String.format("%s: workload duration %d less than 0. Calculating duration in days (%d minutes).",
                    this.name, duration, this.duration);
        }
        else {
            this.duration = DEFAULT_DURATION;
            message = String.format("%s: workload duration set to 0. Using default duration of %d minutes",
                    this.name, DEFAULT_DURATION);
        }
        LOG.info(message);
        System.out.println(message);
    }

    public Properties getAsProperties() {
        Properties properties = new Properties();
        properties.setProperty("name", name);
        properties.setProperty("dataset", dataset);
        properties.setProperty("schema", schema);
        properties.setProperty("insertPCT", String.valueOf(operationPct.getInsert()));
        properties.setProperty("updatePCT", String.valueOf(operationPct.getUpdate()));
        properties.setProperty("deletePCT", String.valueOf(operationPct.getDelete()));
        properties.setProperty("workerThreads", String.valueOf(workerThreads));
        properties.setProperty("preloadThreads", String.valueOf(preloadThreads));
        properties.setProperty("duration", duration + " minutes");
        properties.setProperty("preloadBatchSize", String.valueOf(preloadBatchSize));
        properties.setProperty("workerBatchSize", String.valueOf(workerBatchSize));
        properties.setProperty("workerThreadSleep", workerThreadSleep + " ms");
        properties.setProperty("workerBatchSleep", workerBatchSleep + " ms");
        properties.setProperty("allTablesAllThreads", allTablesAllThreads);

        return properties;
    }
}
