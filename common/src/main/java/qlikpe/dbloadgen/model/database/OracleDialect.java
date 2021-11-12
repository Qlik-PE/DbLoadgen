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
import qlikpe.dbloadgen.model.output.OutputBuffer;
import qlikpe.dbloadgen.model.output.OutputBufferMap;
import qlikpe.dbloadgen.model.output.TextBuffer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * A class that interfaces with Oracle and databases whose
 * SQL syntax for the operations we require are compatible.
 */
public class OracleDialect extends Database {
    private static final Logger LOG = LogManager.getLogger(OracleDialect.class);

    /**
     * Default Constructor.
     */
    public OracleDialect(String databaseType) {
        super();
        LOG.debug("OracleDialect() constructor: Database Type: {}", databaseType);
        setDatabaseType(databaseType);
    }

    /**
     * Surround the query with a code block that will catch an exception,
     * ignoring the exception specified in sqlCode, and rethrowing any
     * others. The idea here is that we will ignore "EXISTS" or
     * "NOT EXISTS" exceptions as appropriate, but error out on
     * any others.
     *
     * @param query the query to execute.
     * @param sqlCode the SQLCODE to ignore.
     * @return the formatted code block.
     */
    private String codeBlock(String query, int sqlCode) {
        String code = String.format("declare%n begin%n");
        code += String.format("execute immediate '%s';%n", query);
        code += String.format("exception when others then%n");
        code += String.format("if SQLCODE = %d then null; else raise; end if;%n", sqlCode);
        code += String.format("end;%n");

        return code;
    }

    @Override
    public String autoIncrementPrimaryKey(String columnName) {
        return String.format("%s NUMBER GENERATED ALWAYS AS IDENTITY", columnName);
    }

    @Override
    public String selectRandomRows(String keyColumnNames, String schemaName, String tableName, int limit) {
        /*
        The following is very random, but may not be efficient on large tables.
        "SELECT keyColumnNames FROM schema.table SAMPLE(pct)" would be more efficient overall,
        but SAMPLE returns a percentage of the total table, not a row count.
        Another option: "SELECT %s FROM ( SELECT %s FROM %s.%s ORDER BY DBMS_RANDOM.VALUE ) WHERE ROWNUM < %d"
        */

        int val = Integer.parseInt(getUnsignedInteger().nextValue()) % 2;
        String sortOrder;
        if (val == 0)
            sortOrder = "ASC";
        else sortOrder = "DESC";

        return String.format("SELECT * FROM (SELECT %s FROM %s.%s ORDER BY %s %s) WHERE ROWNUM < %d",
                keyColumnNames, schemaName, tableName, quoteName(getDbRandom()), sortOrder, limit+1);
    }

    /**
     * create the target schema if it doesn't already exist. In oracle terms, a schema
     * is a "user".
     *  @param connection the database connection to use.
     * @param schemaName the name of the schema
     * @return
     */
    @Override
    public boolean createSchema(Connection connection, String schemaName) {
        TextBuffer outputBuffer =
                (TextBuffer) OutputBufferMap.getInstance().getOutputBufferByName(OutputBufferMap.INITIALIZE_SCHEMA);

        String password = "attunity";
        String query = String.format("CREATE USER %s IDENTIFIED BY %s", quoteName(schemaName), quoteName(password));
        String code = codeBlock(query, -1920);


        try {
            Statement stmt = connection.createStatement();
            stmt.execute(code);
            outputBuffer.addLine(OutputBuffer.Priority.INFO, "created schema " + schemaName);
            LOG.debug("create schema succeeded: " + query);
            stmt.close();
        } catch (SQLException e) {
            String message = String.format("Failed to create schema %s: %s", schemaName, e.getMessage());
            outputBuffer.addLine(OutputBuffer.Priority.ERROR, message);
            LOG.error("Failed to create schema: " + code, e.getMessage());
        }

        query = String.format("ALTER USER %s QUOTA UNLIMITED ON USERS", quoteName(schemaName));

        try {
            Statement stmt = connection.createStatement();
            stmt.execute(query);
            outputBuffer.addLine(OutputBuffer.Priority.INFO, "alter user quota succeeded for schema " + schemaName);
            LOG.debug("alter user quota succeeded: " + query);
            stmt.close();
        } catch (SQLException e) {
            String message = String.format("Failed to alter user quota for schema %s: %s", schemaName, e.getMessage());
            outputBuffer.addLine(OutputBuffer.Priority.ERROR, message);

            LOG.error("Failed to alter user quota: " + query, e.getMessage());
        }

        query = String.format("GRANT UNLIMITED TABLESPACE TO %s", quoteName(schemaName));

        try {
            Statement stmt = connection.createStatement();
            stmt.execute(query);
            outputBuffer.addLine(OutputBuffer.Priority.INFO, "grant unlimited tablespace succeeded for user " + schemaName);
            LOG.debug("grant tablespace succeeded: " + query);
            stmt.close();
        } catch (SQLException e) {
            String message = String.format("Failed to grant tablespace for user %s: %s", schemaName, e.getMessage());
            outputBuffer.addLine(OutputBuffer.Priority.ERROR, message);
            LOG.error("Failed to grant tablespace: " + query, e.getMessage());
        }

        return false;
    }

    /**
     * Drop the target schema. In oracle terms, this is a "user".
     *
     * @param connection the database connection to use.
     * @param schemaName the name of the schema.
     * @return true on success, false otherwise.
     */
    @Override
    public boolean dropSchema(Connection connection, String schemaName) {
        boolean rval;
        TextBuffer outputBuffer =
                (TextBuffer)OutputBufferMap.getInstance().getOutputBufferByName(OutputBufferMap.CLEANUP);
        String query = String.format("DROP USER %s CASCADE", quoteName(schemaName));
        String code = codeBlock(query, -1918);

        try {
            Statement stmt = connection.createStatement();
            stmt.execute(code);
            rval = true;
            outputBuffer.addLine(OutputBuffer.Priority.INFO, "dropped user schema " + schemaName);
            LOG.debug("drop schema succeeded: " + query);
            stmt.close();
        } catch (SQLException e) {
            String exception = e.getMessage().toLowerCase();
            String message;
            if (exception.contains("not found")) {
                rval = true;
                message = String.format("Schema %s was not found: %s", schemaName, e.getMessage());

            } else {
                rval = false;
                message = String.format("Failed to drop schema %s: %s", schemaName, e.getMessage());
            }
            outputBuffer.addLine(OutputBuffer.Priority.WARNING, message);
            LOG.warn(message);
        }
        return rval;
    }

    /**
     * Drop the specified table.
     *
     * @param connection the database connection to use.
     * @param table      the table we need to drop.
     * @return true on success, false otherwise.
     */
    @Override
    public boolean dropTable(Connection connection, Table table) {
        boolean rval;
        TextBuffer outputBuffer =
                (TextBuffer)OutputBufferMap.getInstance().getOutputBufferByName(OutputBufferMap.CLEANUP);

        String query = String.format("DROP TABLE %s.%s PURGE",
                quoteName(table.getSchemaName()), quoteName(table.getName()));
        String code = codeBlock(query, -942);

        try {
            Statement stmt = connection.createStatement();
            stmt.execute(code);
            rval = true;
            outputBuffer.addLine(OutputBuffer.Priority.INFO, "successfully dropped table " + table.getName());
            LOG.debug("drop table succeeded: " + query);
            stmt.close();
        } catch (SQLException e) {
            String exception = e.getMessage().toLowerCase();
            String message;
            if (exception.contains("not found")) {
                rval = true;
                message = String.format("Table %s was not found: %s", table.getName(), e.getMessage());

            } else {
                rval = false;
                message = String.format("Failed to drop table %s: %s", table.getName(), e.getMessage());
            }
            outputBuffer.addLine(OutputBuffer.Priority.WARNING, message);
            LOG.warn(message);
        }
        return rval;
    }

    /**
     * Create the specified table.
     *
     * @param connection the database connection to use.
     * @param table      the table metadata.
     * @return true on success.
     */
    @Override
    public boolean createTable(Connection connection, Table table) {
        boolean rval;
        String code;
        int numKeyColumns;
        String sep = "";
        List<Column> columns = table.getColumns();

        if ((table.getKeyColumns() != null))
            numKeyColumns = table.getKeyColumns().size();
        else numKeyColumns = 0;

        String query;

        query = String.format("CREATE TABLE %s.%s (",
                quoteName(table.getSchemaName()), quoteName(table.getName()));


        if (numKeyColumns == 0) {
            // no key columns specified, so add auto incrementing primary key
            query = String.format("%s %s, ", query, autoIncrementPrimaryKey(quoteName(defaultKeyColumn)));
        }


        for (Column column : columns) {
            query = String.format("%s%s %s %s %s", query, sep, quoteName(column.getName()), column.getDatabaseType(),
                    column.isNullable() ? "" : "NOT NULL");
            sep = ",";
        }

        // add primary key constraint
        if (numKeyColumns > 0) {
            sep = "";
            String pk = ", PRIMARY KEY(";
            for (String columnName : table.getKeyColumns()) {
                pk = String.format("%s%s %s", pk, sep, quoteName(columnName));
                sep = ",";
            }
            pk = pk + ")";
            query = String.format("%s%s", query, pk);
        }
        query = query + ")";

        code = codeBlock(query, -955);

        try {
            Statement stmt = connection.createStatement();
            stmt.execute(code);
            LOG.debug("create table succeeded: " + query);
            stmt.close();
            rval = true;
        } catch (SQLException e) {
            LOG.error("Failed to create table: " + code, e.getMessage());
            rval = false;
        }

        return rval;
    }

    /**
     * Create an index on the specified table.
     *
     * @param connection  the database connection to use.
     * @param table       the table we will create the index on.
     * @param columnNames a comma delimited list of column names.
     */
    @Override
    public void createIndex(Connection connection, Table table, String columnNames) {
        String indexName = String.format("index_%s_%s",
                table.getName(), columnNames.replace(',', '_'));

        String[] names = columnNames.split(",");

        String quotedNames = "";
        String sep = "";
        for(String s : names) {
            quotedNames = String.format("%s%s%s", quotedNames, sep, s);
            sep = ",";
        }

        String query = String.format("CREATE INDEX %s ON %s.%s (%s)",
                quoteName(indexName), quoteName(table.getSchemaName()),
                quoteName(table.getName()), quotedNames);

        String code = codeBlock(query, -955);

        try {
            Statement stmt = connection.createStatement();
            stmt.execute(code);
            LOG.debug("create index succeeded: " + query);
            stmt.close();
        } catch (SQLException e) {
            LOG.error("Failed to create index: " + code, e.getMessage());
        }
    }

    /**
     * Add a column to the specified table.
     *
     * @param connection the database connection to use.
     * @param table      the  table to which we will add a column.
     * @param columnName the name of the column to add.
     * @param colType    the datatype of the column.
     */
    @Override
    public void addColumn(Connection connection, Table table, String columnName, String colType) {
        String query = String.format("ALTER TABLE %s.%s ADD %s %s",
                quoteName(table.getSchemaName()), quoteName(table.getName()),
                quoteName(columnName), colType);
        String code = codeBlock(query, -1430);

        try {
            Statement stmt = connection.createStatement();
            stmt.execute(code);
            LOG.debug("add column succeeded: " + query);
            stmt.close();
        } catch (SQLException e) {
            LOG.error("Failed to add column: " + code, e.getMessage());
        }
    }


    /**
     * Set the data type mappings for Oracle.
     * @param dataTypeMapper an instance of DataTypeMapper.
     */
    @Override
    public void overrideColumnTypes(DataTypeMapper dataTypeMapper) {
        dataTypeMapper.overrideDefaultDatabaseType("CHAR", "CHAR");
        dataTypeMapper.overrideDefaultDatabaseType("NCHAR", "NCHAR");
        dataTypeMapper.overrideDefaultDatabaseType("VARCHAR", "VARCHAR2");
        dataTypeMapper.overrideDefaultDatabaseType("NVARCHAR", "NVARCHAR2");
        dataTypeMapper.overrideDefaultDatabaseType("BOOLEAN", "VARCHAR2");
        dataTypeMapper.overrideDefaultDatabaseType("BYTES", "RAW");
        dataTypeMapper.overrideDefaultDatabaseType("VARBYTES", "RAW");
        dataTypeMapper.overrideDefaultDatabaseType("DATE", "DATE");
        dataTypeMapper.overrideDefaultDatabaseType("TIME", "DATE");
        dataTypeMapper.overrideDefaultDatabaseType("DATETIME", "TIMESTAMP");
        dataTypeMapper.overrideDefaultDatabaseType("INT1", "INTEGER");
        dataTypeMapper.overrideDefaultDatabaseType("INT2", "INTEGER");
        dataTypeMapper.overrideDefaultDatabaseType("INT3", "INTEGER");
        dataTypeMapper.overrideDefaultDatabaseType("INT4", "INTEGER");
        dataTypeMapper.overrideDefaultDatabaseType("INT8", "INTEGER");
        dataTypeMapper.overrideDefaultDatabaseType("NUMERIC", "NUMBER");
        dataTypeMapper.overrideDefaultDatabaseType("FLOAT", "BINARY_FLOAT");
        dataTypeMapper.overrideDefaultDatabaseType("DOUBLE", "BINARY_DOUBLE");
        dataTypeMapper.overrideDefaultDatabaseType("UINT1", "INTEGER");
        dataTypeMapper.overrideDefaultDatabaseType("UINT2", "INTEGER");
        dataTypeMapper.overrideDefaultDatabaseType("UINT3", "INTEGER");
        dataTypeMapper.overrideDefaultDatabaseType("UINT4", "INTEGER");
        dataTypeMapper.overrideDefaultDatabaseType("UINT8", "INTEGER");
        dataTypeMapper.overrideDefaultDatabaseType("BLOB", "BLOB");
        dataTypeMapper.overrideDefaultDatabaseType("CLOB", "CLOB");
        dataTypeMapper.overrideDefaultDatabaseType("NCLOB", "NCLOB");
    }

}
