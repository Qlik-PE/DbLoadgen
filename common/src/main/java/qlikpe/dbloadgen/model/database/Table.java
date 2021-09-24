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
package qlikpe.dbloadgen.model.database;

import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import qlikpe.dbloadgen.model.workload.TableConfig;
import qlikpe.dbloadgen.model.workload.TableStats;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * Manages database table metadata. This data is parsed from the
 *  * dataset/tableName.yml files.
 */
@Getter
@Setter
public class Table {
    private static final Logger LOG = LogManager.getLogger(Table.class);

    private String name;
    private ArrayList<Column> columns;  // a list of database columns
    private ArrayList<String> keyColumns;
    private ArrayList<String> data;
    private TableStats tableStats;
    private RandomRows randomRows;
    private String schemaName;
    private Database database;
    private TableConfig tableConfig;
    private int preloadCount;



    /**
     * Default constructor.
     */
    public Table() {
        randomRows = new RandomRows(this);
        tableStats = new TableStats();
        preloadCount = 0;
    }

    /**
     * Copy constructor. Called by WorkerThread because the
     * worker threads need copies of the tables for concurrency reasons.
     * @param table the instance of Table to copy
     * @param database an instance of Database to assign.
     */
    public Table(Table table, Database database) {
        this.name = table.getName();
        this.tableStats = table.getTableStats();

        this.schemaName = table.getSchemaName();
        this.tableConfig = table.getTableConfig();
        this.preloadCount = table.getPreloadCount();
        this.database = database;

        this.data = table.data; // shouldn't need a deep copy of this

        // need to create a copy of these collections for concurrency reasons.
        this.columns = new ArrayList<>(table.getColumns());
        if (table.getKeyColumns() != null)
            this.keyColumns = new ArrayList<>(table.getKeyColumns());
        else this.keyColumns = null;
        this.randomRows = new RandomRows(this);
    }

    public List<ColumnValue> getNextRandomRow(Connection connection) {
        return randomRows.getNextRandomRow(connection);
    }

    public Column getColumnByName(String columnName) {
        for(Column column : columns) {
            if (column.getName().equals(columnName)) {
                return column;
            }
        }
        LOG.error("column name {} not found in table {}", columnName, name);
        return null;
    }

    public void setColumnTypes(DataTypeMapper dataTypeMapper) {
        for(Column column : columns) {
            column.configureColumn(dataTypeMapper);
        }
    }
}
