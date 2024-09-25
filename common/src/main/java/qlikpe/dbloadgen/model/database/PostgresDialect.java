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
 * A class that interfaces with Postgres and databases whose
 * SQL syntax for the operations we require are compatible.
 */
public class PostgresDialect extends Database {
    private static final Logger LOG = LogManager.getLogger(PostgresDialect.class);

    /**
     * Default Constructor.
     */
    public PostgresDialect(String databaseType) {
        super();
        LOG.debug("PostgresDialect() constructor: Database Type: {}", databaseType);
        setDatabaseType(databaseType);
        setSupportsExists(true);
        setSupportsDropSchemaCascade(true);
        setSupportsDropTableCascade(true);
        setAlterTableColumnKeyword(true);
        setSupportsUnsignedInts(false);
        setQuoteChar('"');
    }

    @Override
    public String autoIncrementPrimaryKey(String columnName) {
        return String.format("%s SERIAL PRIMARY KEY", columnName);
    }

    @Override
    public String selectRandomRows(String keyColumnNames, String schemaName, String tableName, int limit) {
        // more random, less efficient option: "SELECT %s FROM %s.%s ORDER BY RANDOM() LIMIT %d"

        return String.format("SELECT %s FROM %s.%s ORDER BY %s %s LIMIT %d",
                keyColumnNames, schemaName, tableName, quoteName(getDbRandom()), randomSortOrder(), limit);
    }


    /**
     * Set the data type mappings for Postgres.
     * @param dataTypeMapper an instance of DataTypeMapper.
     */
    @Override
    public void overrideColumnTypes(DataTypeMapper dataTypeMapper) {
        dataTypeMapper.overrideDefaultDatabaseType("CHAR", "CHAR");
        dataTypeMapper.overrideDefaultDatabaseType("NCHAR", "CHAR");
        dataTypeMapper.overrideDefaultDatabaseType("VARCHAR", "VARCHAR");
        dataTypeMapper.overrideDefaultDatabaseType("NVARCHAR", "VARCHAR");
        dataTypeMapper.overrideDefaultDatabaseType("BOOLEAN", "BOOLEAN");
        dataTypeMapper.overrideDefaultDatabaseType("BYTES", "BYTEA");
        dataTypeMapper.overrideDefaultDatabaseType("VARBYTES", "BYTEA");
        dataTypeMapper.overrideDefaultDatabaseType("DATE", "DATE");
        dataTypeMapper.overrideDefaultDatabaseType("TIME", "TIME");
        dataTypeMapper.overrideDefaultDatabaseType("DATETIME", "TIMESTAMP");
        dataTypeMapper.overrideDefaultDatabaseType("INT1", "SMALLINT");
        dataTypeMapper.overrideDefaultDatabaseType("INT2", "SMALLINT");
        dataTypeMapper.overrideDefaultDatabaseType("INT3", "INTEGER");
        dataTypeMapper.overrideDefaultDatabaseType("INT4", "INTEGER");
        dataTypeMapper.overrideDefaultDatabaseType("INT8", "BIGINT");
        dataTypeMapper.overrideDefaultDatabaseType("NUMERIC", "NUMERIC");
        dataTypeMapper.overrideDefaultDatabaseType("FLOAT", "REAL");
        dataTypeMapper.overrideDefaultDatabaseType("DOUBLE", "DOUBLE PRECISION");
        dataTypeMapper.overrideDefaultDatabaseType("UINT1", "SMALLINT");
        dataTypeMapper.overrideDefaultDatabaseType("UINT2", "INTEGER");
        dataTypeMapper.overrideDefaultDatabaseType("UINT3", "INTEGER");
        dataTypeMapper.overrideDefaultDatabaseType("UINT4", "BIGINT");
        dataTypeMapper.overrideDefaultDatabaseType("UINT8", "BIGINT");
        dataTypeMapper.overrideDefaultDatabaseType("BLOB", "BYTEA");
        dataTypeMapper.overrideDefaultDatabaseType("CLOB", "TEXT");
        dataTypeMapper.overrideDefaultDatabaseType("NCLOB", "TEXT");
    }

}
