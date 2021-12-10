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

/**
 * A class that interfaces with Microsoft SQL Server and databases whose
 * SQL syntax for the operations we require are compatible.
 */
public class SqlServerDialect extends Database {
    private static final Logger LOG = LogManager.getLogger(SqlServerDialect.class);

    /**
     * Default Constructor.
     */
    public SqlServerDialect(String databaseType) {
        super();
        LOG.trace("SqlServerDialect() constructor: Database Type: {}", databaseType);
        setDatabaseType(databaseType);
    }

    @Override
    public String autoIncrementPrimaryKey(String columnName) {
        return String.format("%s INTEGER NOT NULL IDENTITY PRIMARY KEY", columnName);
    }

    @Override
    public String selectRandomRows(String keyColumnNames, String schemaName, String tableName, int limit) {
        // more random, less efficient option: "SELECT TOP %d %s FROM %s.%s ORDER BY NEWID()"
        int val = Integer.parseInt(getUnsignedInteger().nextValue()) % 2;
        String sortOrder;
        if (val == 0)
            sortOrder = "ASC";
        else sortOrder = "DESC";

        return String.format("SELECT TOP %d %s FROM %s.%s ORDER BY %s %s",
                limit, keyColumnNames, schemaName, tableName, quoteName(getDbRandom()), sortOrder);

    }

    /**
     * create the target schema if it doesn't already exist.
     *  @param connection the database connection to use.
     * @param schemaName the name of the schema
     * @return success or failure.
     */
    @Override
    public boolean createSchema(Connection connection, String schemaName) {
        boolean rval = true;
        String query = String.format("IF NOT EXISTS ( SELECT * FROM sys.schemas WHERE name = N'%s' )" +
                "EXEC('CREATE SCHEMA [%s]')", schemaName, schemaName);

        try {
            Statement stmt = connection.createStatement();
            stmt.execute(query);
            LOG.debug("create schema succeeded: " + query);
            stmt.close();
        } catch (SQLException e) {
            rval = false;
            LOG.error("Failed to create schema: " + query, e);
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

        /*
         * Dropping a table will fail if that table is marked for replication. This "pre-drop" step
         * attempts to unmark the table so we can drop it without affecting other replication-related
         * settings.
         *
         * See: https://pawjershauge.blogspot.com/2012/06/spmsunmarkreplinfo-transact-sql.html
         */
        String removeReplication =
                String.format("IF OBJECT_ID(N'%s.%s', N'U') IS NOT NULL EXEC sp_msunmarkreplinfo [%s], [%s]",
                table.getSchemaName(), table.getName(),
                table.getName(), table.getSchemaName());

        try {
            LOG.debug("dropTable(): attempting to remove replication from table {}", table.getName());
            Statement stmt = connection.createStatement();
            stmt.execute(removeReplication);
            //LOG.debug("dropTable(): remove replication succeeded: " + removeReplication);
            stmt.close();
        } catch (SQLException e) {
            String message = String.format("Failed to remove replication %s: %s", table.getName(), e.getMessage());
            outputBuffer.addLine(OutputBuffer.Priority.WARNING, message);
            LOG.warn(message);
        }

        String query = String.format("IF OBJECT_ID(N'%s.%s', N'U') IS NOT NULL DROP TABLE [%s].[%s]",
                table.getSchemaName(), table.getName(),
                table.getSchemaName(), table.getName());

        try {
            Statement stmt = connection.createStatement();
            stmt.execute(query);
            rval = true;
            outputBuffer.addLine(OutputBuffer.Priority.INFO, "successfully dropped table: " + table.getName());
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
     * Drop the target schema.
     *
     * @param connection the database connection to use.
     * @param schemaName the name of the schema.
     * @return true on success, false otherwise.
     */
    @Override
    public boolean dropSchema(Connection connection, String schemaName) {
        boolean rval;
        TextBuffer outputBuffer =
                (TextBuffer) OutputBufferMap.getInstance().getOutputBufferByName(OutputBufferMap.CLEANUP);
        String query = String.format("IF EXISTS (SELECT * FROM sys.schemas WHERE name = N'%s') DROP SCHEMA [%s]",
                schemaName, schemaName);

        try {
            Statement stmt = connection.createStatement();
            stmt.execute(query);
            rval = true;
            outputBuffer.addLine(OutputBuffer.Priority.INFO, "dropped schema " + schemaName);
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
     * Set the data type mappings for SQL Server.
     * @param dataTypeMapper an instance of DataTypeMapper.
     */
    @Override
    public void overrideColumnTypes(DataTypeMapper dataTypeMapper) {
        dataTypeMapper.overrideDefaultDatabaseType("CHAR", "CHAR");
        dataTypeMapper.overrideDefaultDatabaseType("NCHAR", "NCHAR");
        dataTypeMapper.overrideDefaultDatabaseType("VARCHAR", "VARCHAR");
        dataTypeMapper.overrideDefaultDatabaseType("NVARCHAR", "NVARCHAR");
        dataTypeMapper.overrideDefaultDatabaseType("BOOLEAN", "BIT");
        dataTypeMapper.overrideDefaultDatabaseType("BYTES", "BINARY");
        dataTypeMapper.overrideDefaultDatabaseType("VARBYTES", "VARBINARY");
        dataTypeMapper.overrideDefaultDatabaseType("DATE", "DATE");
        dataTypeMapper.overrideDefaultDatabaseType("TIME", "TIME");
        dataTypeMapper.overrideDefaultDatabaseType("DATETIME", "DATETIME2");
        dataTypeMapper.overrideDefaultDatabaseType("INT1", "TINYINT");
        dataTypeMapper.overrideDefaultDatabaseType("INT2", "SMALLINT");
        dataTypeMapper.overrideDefaultDatabaseType("INT3", "INT");
        dataTypeMapper.overrideDefaultDatabaseType("INT4", "INT");
        dataTypeMapper.overrideDefaultDatabaseType("INT8", "BIGINT");
        dataTypeMapper.overrideDefaultDatabaseType("NUMERIC", "NUMERIC");
        dataTypeMapper.overrideDefaultDatabaseType("FLOAT", "FLOAT(24)");
        dataTypeMapper.overrideDefaultDatabaseType("DOUBLE", "FLOAT(53)");
        dataTypeMapper.overrideDefaultDatabaseType("UINT1", "SMALLINT");
        dataTypeMapper.overrideDefaultDatabaseType("UINT2", "INT");
        dataTypeMapper.overrideDefaultDatabaseType("UINT3", "INT");
        dataTypeMapper.overrideDefaultDatabaseType("UINT4", "BIGINT");
        dataTypeMapper.overrideDefaultDatabaseType("UINT8", "BIGINT");
        dataTypeMapper.overrideDefaultDatabaseType("BLOB", "IMAGE");
        dataTypeMapper.overrideDefaultDatabaseType("CLOB", "TEXT");
        dataTypeMapper.overrideDefaultDatabaseType("NCLOB", "NTEXT");
    }


}
