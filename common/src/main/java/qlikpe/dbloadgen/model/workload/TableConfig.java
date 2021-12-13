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
import qlikpe.dbloadgen.model.database.Table;

import java.util.ArrayList;
import java.util.List;

/**
 * Workload-related configuration information for each table.
 */
@Getter
@Setter
public class TableConfig {
    private static final Logger LOG = LogManager.getLogger(TableConfig.class);

    private String name;
    private String preload; // "data" or number of rows to generate.
    private String updateColumns;
    private OperationPct operationPct;  // optional: will default to WorkloadConfig pct.
    private Table parent;

    @Getter(lazy=true, onMethod = @__({@SuppressWarnings("unchecked")}))
    private final List<String> updateColumnNames = initColumnNames();

    public TableConfig() { }

    public List<String> initColumnNames() {
        List<String> rval;
        if ((updateColumns == null) || (updateColumns.length() == 0)) {
            // no update columns specified
            rval = null;
        } else {
            rval = new ArrayList<>();
            String[] colsArray = updateColumns.split(",");
            for (String col : colsArray) {
                assert parent != null;
                if (parent.getColumnByName(col) != null) {
                    rval.add(col);
                } else {
                    LOG.warn("Ignoring update column {} because it was not found in table {}.", col, getName());
                }
            }
            if (rval.size() == 0)
                // all update columns specified were invalid
                rval = null;
        }
        return rval;
    }
}
