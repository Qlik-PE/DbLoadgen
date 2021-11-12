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

import java.util.List;

/**
 * The workload configuration that is parsed from the workload.yml file.
 * Note that this class is constructed by te yaml parser.
 */
@Getter
@Setter
public class WorkloadConfig {
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
}
