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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A class that interfaces with MySQL and databases whose
 * SQL syntax for the operations we require are compatible.
 */
public class MySqlDialect extends Database {
    private static final Logger LOG = LogManager.getLogger(MySqlDialect.class);

    /**
     * Default Constructor.
     */
    public MySqlDialect(String databaseType) {
        super();
        LOG.debug("MySqlDialect() constructor: Database Type: {}", databaseType);
        setDatabaseType(databaseType);
    }

    @Override
    public String autoIncrementPrimaryKey(String columnName) {
        return String.format("%s INT AUTO_INCREMENT PRIMARY KEY", columnName);
    }

    @Override
    public String selectRandomRows(String keyColumnNames, String schemaName, String tableName, int limit) {
        // more random, less efficient option:  "SELECT %s FROM %s.%s ORDER BY RAND() LIMIT %d"
        int val = Integer.parseInt(getUnsignedInteger().nextValue()) % 2;
        String sortOrder;
        if (val == 0)
            sortOrder = "ASC";
        else sortOrder = "DESC";

        return String.format("SELECT %s FROM %s.%s ORDER BY %s %s LIMIT %d",
                keyColumnNames, schemaName, tableName, quoteName(getDbRandom()), sortOrder, limit);
    }

    /**
     * Set the data type mappings for MySQL.
     * @param dataTypeMapper an instance of DataTypeMapper.
     */
    @Override
    public void overrideColumnTypes(DataTypeMapper dataTypeMapper) {
        dataTypeMapper.overrideDefaultDatabaseType("CHAR", "CHAR");
        dataTypeMapper.overrideDefaultDatabaseType("NCHAR", "NCHAR");
        dataTypeMapper.overrideDefaultDatabaseType("VARCHAR", "VARCHAR");
        dataTypeMapper.overrideDefaultDatabaseType("NVARCHAR", "NVARCHAR");
        dataTypeMapper.overrideDefaultDatabaseType("BOOLEAN", "BOOLEAN");
        dataTypeMapper.overrideDefaultDatabaseType("BYTES", "BINARY");
        dataTypeMapper.overrideDefaultDatabaseType("VARBYTES", "VARBINARY");
        dataTypeMapper.overrideDefaultDatabaseType("DATE", "DATE");
        dataTypeMapper.overrideDefaultDatabaseType("TIME", "TIME");
        dataTypeMapper.overrideDefaultDatabaseType("DATETIME", "DATETIME");
        dataTypeMapper.overrideDefaultDatabaseType("INT1", "TINYINT");
        dataTypeMapper.overrideDefaultDatabaseType("INT2", "SMALLINT");
        dataTypeMapper.overrideDefaultDatabaseType("INT3", "MEDIUMINT");
        dataTypeMapper.overrideDefaultDatabaseType("INT4", "INTEGER");
        dataTypeMapper.overrideDefaultDatabaseType("INT8", "BIGINT");
        dataTypeMapper.overrideDefaultDatabaseType("NUMERIC", "NUMERIC");
        dataTypeMapper.overrideDefaultDatabaseType("FLOAT", "FLOAT");
        dataTypeMapper.overrideDefaultDatabaseType("DOUBLE", "DOUBLE");
        dataTypeMapper.overrideDefaultDatabaseType("UINT1", "TINYINT UNSIGNED");
        dataTypeMapper.overrideDefaultDatabaseType("UINT2", "SMALLINT UNSIGNED");
        dataTypeMapper.overrideDefaultDatabaseType("UINT3", "MEDIUMINT UNSIGNED");
        dataTypeMapper.overrideDefaultDatabaseType("UINT4", "INTEGER UNSIGNED");
        dataTypeMapper.overrideDefaultDatabaseType("UINT8", "BIGINT UNSIGNED");
        dataTypeMapper.overrideDefaultDatabaseType("BLOB", "BLOB");
        dataTypeMapper.overrideDefaultDatabaseType("CLOB", "TEXT");
        dataTypeMapper.overrideDefaultDatabaseType("NCLOB", "TEXT");
    }
}
