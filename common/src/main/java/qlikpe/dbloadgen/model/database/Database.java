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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import qlikpe.dbloadgen.model.initializer.UnsignedInteger;
import qlikpe.dbloadgen.model.output.OutputBuffer;
import qlikpe.dbloadgen.model.output.OutputBufferMap;
import qlikpe.dbloadgen.model.output.TextBuffer;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * An abstract base class for the databases that we will support.
 */
@Getter
public abstract class Database {
    public enum OperationType {INSERT, UPDATE, DELETE}

    private static final Logger LOG = LogManager.getLogger(Database.class);
    public static final String defaultKeyColumn = "id_";
    private static final String dbRandom = "dbrandom";
    private static final String randomType = "integer";
    private String databaseType;
    private boolean supportsExists;
    private boolean supportsCascade;
    private boolean alterTableColumnKeyword;
    private boolean supportsUnsignedInts;
    private char quoteChar;
    private final UnsignedInteger unsignedInteger = new UnsignedInteger(0, 200000);


    /**
     * Constructor.
     */
    public Database() {
    }

    /**
     * Create an instance of a database object.
     * Every thread should have its own.
     *
     * @return an instance of Database.
     */
    public static Database databaseFactory(String databaseType) {
        Database database;
        switch (databaseType.toLowerCase()) {
            case "mysql":
                database = new MySqlDialect("MySQL");
                database.setSupportsExists(true);
                database.setSupportsCascade(true);
                database.setAlterTableColumnKeyword(true);
                database.setSupportsUnsignedInts(true);
                database.setQuoteChar('`');
                break;
            case "h2":
                database = new MySqlDialect("H2");
                database.setSupportsExists(true);
                database.setSupportsCascade(true);
                database.setAlterTableColumnKeyword(true);
                database.setSupportsUnsignedInts(false);
                database.setQuoteChar('`');
                break;
            case "postgres":
            case "postgresql":
                database = new PostgresDialect("PostgreSQL");
                database.setSupportsExists(true);
                database.setSupportsCascade(true);
                database.setAlterTableColumnKeyword(true);
                database.setSupportsUnsignedInts(false);
                database.setQuoteChar('"');
                break;
            case "oracle":
                database = new OracleDialect("Oracle");
                database.setSupportsExists(false);
                database.setSupportsCascade(false);
                database.setAlterTableColumnKeyword(false);
                database.setSupportsUnsignedInts(false);
                database.setQuoteChar('"');
                break;
            case "sqlserver":
                database = new SqlServerDialect("SQL Server");
                database.setSupportsExists(false);
                database.setSupportsCascade(false);
                database.setAlterTableColumnKeyword(false);
                database.setSupportsUnsignedInts(false);
                database.setQuoteChar('"');
                break;
            default:
                String message = String.format("unrecognized database type specified: %s", databaseType);
                LOG.error(message);
                database = null;
        }
        return database;
    }

    /**
     * Get the name of the column used for randomizing data.
     * @return the column name as a String.
     */
    public String getDbRandom() { return dbRandom; }

    /**
     * Return the type of database we are mapping to as a String.
     *
     * @return the database type.
     */
    public String getDatabaseType() {
        return databaseType;
    }

    /**
     * Set the database type.
     *
     * @param databaseType the database type.
     */
    public void setDatabaseType(String databaseType) {
        this.databaseType = databaseType;
    }

    /**
     * Indicates whether this database dialect supports EXISTS subclauses.
     *
     * @param supportsExists true if EXISTS is supported, false otherwise.
     */
    public void setSupportsExists(boolean supportsExists) {
        this.supportsExists = supportsExists;
    }

    /**
     * Indicates whether this database dialect supports CASCADE subclauses.
     *
     * @param supportsCascade true if CASCADE is supported, false otherwise.
     */
    public void setSupportsCascade(boolean supportsCascade) {
        this.supportsCascade = supportsCascade;
    }

    /**
     * Indicates whether or not this database supports unsigned integer types.
     *
     * @param supportsUnsignedInts true if unsigned integer types are supported, false otherwise.
     */
    public void setSupportsUnsignedInts(boolean supportsUnsignedInts) {
        this.supportsUnsignedInts = supportsUnsignedInts;
    }

    /**
     * Indicates whether or not this database supports unsigned integer types.
     *
     * returns true if unsigned integer types are supported, false otherwise.
     */
    public boolean getSupportsUnsignedInts() { return supportsUnsignedInts; }

    /**
     * Set the character to wrap column and table names with..
     *
     * @param quoteChar the character to use to surround database identifiers.
     */
    public void setQuoteChar(char quoteChar) {
        this.quoteChar = quoteChar;
    }

    /**
     * Get the character used to surround identifiers.
     *
     * returns the quote character as a character.
     */
    public char getQuoteChar() { return quoteChar; }

    /**
     * Surround a database identifier with the quote character specific to this database.
     * @param name the database identifier.
     * @return a quoted version of name.
     */
    public String quoteName(String name) {
        return String.format("%c%s%c", quoteChar, name, quoteChar);
    }



    /**
     * Indicates whether this database requires the COLUMN keyword in ALTER TABLE statements..
     *
     * @param alterTableColumnKeyword true if COLUMN is required, false otherwise.
     */
    public void setAlterTableColumnKeyword(boolean alterTableColumnKeyword) {
        this.alterTableColumnKeyword = alterTableColumnKeyword;
    }

    /**
     * Extracts the column names and values from the result set returned from a query.
     * This class returns a nested List ... the entries in the outer list each represents
     * the data returned from a row, with each row being represented by a List of
     * columnName/value pairs.
     *
     * @param rs the result set returned from a query.
     * @return a List of Lists.
     * @throws SQLException if there is a problem with the result set.
     */
    protected LinkedList<List<ColumnValue>> getRowValues(ResultSet rs) throws SQLException {
        LinkedList<List<ColumnValue>> rowValues = null;
        if (rs != null) {
            rowValues = new LinkedList<>();
            ResultSetMetaData metadata = rs.getMetaData();
            int columnCount = metadata.getColumnCount();
            if (rs.next()) {
                // there is at least one row in the result set.
                do {
                    List<ColumnValue> row = new ArrayList<>();
                    for (int i = 1; i <= columnCount; i++) {
                        row.add(new ColumnValue(metadata.getColumnName(i), rs.getString(i)));
                    }
                    rowValues.add(row);
                } while (rs.next());
            } else {
                LOG.info("returned result set was empty!!!");
            }
        } else {
            LOG.error("result set was null!!!");
        }
        return rowValues;
    }

    /**
     * Add a column to the specified table that will contain a random integer.
     * This value will be used to select random rows fromm the table in question.
     * While not as completely random, dooing it this way vs. using the database's
     * builtin RANDOM() function during a select is far more efficient
     * for large tables.
     *
     * @param connection the database connection to use.
     * @param table      the table where we will add the column.
     */
    public void addRandomizer(Connection connection, Table table) {
        addColumn(connection, table, dbRandom, randomType);
        createIndex(connection, table, dbRandom);
    }


    /**
     * create the target schema if it doesn't already exist.
     *  @param connection the database connection to use.
     * @param schemaName the name of the schema
     * @return true on success;
     */
    public boolean createSchema(Connection connection, String schemaName) {
        boolean rval;
        String query;
        TextBuffer outputBuffer =
                (TextBuffer)OutputBufferMap.getInstance().getOutputBufferByName(OutputBufferMap.INITIALIZE_SCHEMA);


        if (supportsExists)
            query = String.format("CREATE SCHEMA IF NOT EXISTS %s", quoteName(schemaName));
        else query = String.format("CREATE SCHEMA %s", quoteName(schemaName));

        try {
            Statement stmt = connection.createStatement();
            stmt.execute(query);
            outputBuffer.addLine(OutputBuffer.Priority.INFO, "created schema " + schemaName);
            LOG.debug("create schema succeeded: " + query);
            stmt.close();
            rval = true;
        } catch (SQLException e) {
            String message = String.format("Failed to create schema %s: %s", schemaName, e.getMessage());
            outputBuffer.addLine(OutputBuffer.Priority.ERROR, message);
            LOG.error("Failed to create schema: " + query, e);
            rval = false;
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
    public boolean dropSchema(Connection connection, String schemaName) {
        boolean rval;
        TextBuffer outputBuffer =
                (TextBuffer)OutputBufferMap.getInstance().getOutputBufferByName(OutputBufferMap.CLEANUP);
        String query;

        if (supportsExists)
            query = String.format("DROP SCHEMA IF EXISTS %s %s", quoteName(schemaName), supportsCascade ? "CASCADE" : "");
        else query = String.format("DROP SCHEMA %s %s", quoteName(schemaName), supportsCascade ? "CASCADE" : "");


        try {
            Statement stmt = connection.createStatement();
            stmt.execute(query);
            rval = true;
            outputBuffer.addLine(OutputBuffer.Priority.INFO, "successfully dropped schema " + schemaName);
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
     * @return true if successful, false otherwise.
     */
    public boolean dropTable(Connection connection, Table table) {
        boolean rval;
        TextBuffer outputBuffer =
                (TextBuffer)OutputBufferMap.getInstance().getOutputBufferByName(OutputBufferMap.CLEANUP);

        String query;

        if (supportsExists)
            query = String.format("DROP TABLE IF EXISTS %s.%s %s",
                    quoteName(table.getSchemaName()), quoteName(table.getName()),
                    supportsCascade ? "CASCADE" : "");
        else query = String.format("DROP TABLE %s.%s %s",
                quoteName(table.getSchemaName()), quoteName(table.getName()),
                supportsCascade ? "CASCADE" : "");


        try {
            Statement stmt = connection.createStatement();
            stmt.execute(query);
            rval = true;
            outputBuffer.addLine(OutputBuffer.Priority.INFO, "dropped table " + table.getName());
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
    public boolean createTable(Connection connection, Table table) {
        boolean rval;
        int numKeyColumns;
        String sep = "";
        List<Column> columns = table.getColumns();

        if ((table.getKeyColumns() != null))
            numKeyColumns = table.getKeyColumns().size();
        else numKeyColumns = 0;

        String query;

        if (supportsExists)
            query = String.format("CREATE TABLE IF NOT EXISTS %s.%s (",
                    quoteName(table.getSchemaName()), quoteName(table.getName()));
        else query = String.format("CREATE TABLE %s.%s (",
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
        String message;
        TextBuffer outputBuffer =
                (TextBuffer)OutputBufferMap.getInstance().getOutputBufferByName(OutputBufferMap.INITIALIZE_SCHEMA);

        try {
            Statement stmt = connection.createStatement();
            stmt.execute(query);
            message = String.format("created table %s.%s", table.getSchemaName(), table. getName());
            outputBuffer.addLine(OutputBuffer.Priority.INFO, message);
            LOG.debug("create table succeeded: " + query);
            stmt.close();
            rval = true;
        } catch (SQLException e) {
            message = String.format("Failed to create table %s.%s message=%s",
                    table.getSchemaName(), table.getName(), e.getMessage());
            outputBuffer.addLine(OutputBuffer.Priority.ERROR, message);
            LOG.error("Failed to create table: " + query, e);
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
    public void createIndex(Connection connection, Table table, String columnNames) {
        String query;
        String indexName = String.format("index_%s_%s",
                table.getName(), columnNames.replace(',', '_'));

        String[] names = columnNames.split(",");

        String quotedNames = "";
        String sep = "";
        for(String s : names) {
            quotedNames = String.format("%s%s%s", quotedNames, sep, s);
            sep = ",";
        }

        query = String.format("CREATE INDEX %s ON %s.%s (%s)",
                quoteName(indexName), quoteName(table.getSchemaName()), quoteName(table.getName()), quotedNames);

        try {
            Statement stmt = connection.createStatement();
            stmt.execute(query);
            LOG.debug("create index succeeded: " + query);
            stmt.close();
        } catch (SQLException e) {
            LOG.error("Failed to create index: " + query, e);
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
    public void addColumn(Connection connection, Table table, String columnName, String colType) {
        String query;

        query = String.format("ALTER TABLE %s.%s ADD %s %s %s",
                quoteName(table.getSchemaName()), quoteName(table.getName()),
                alterTableColumnKeyword ? "COLUMN" : "", quoteName(columnName), colType);

        try {
            Statement stmt = connection.createStatement();
            stmt.execute(query);
            LOG.debug("add column succeeded: " + query);
            stmt.close();
        } catch (SQLException e) {
            LOG.error("Failed to add column: " + query, e);
        }
    }


    /**
     * Count the rows in a table.
     *
     * @param connection the database connection to use.
     * @param table      the name of the table..
     * @return the number of rows in the table.
     */
    public long countRows(Connection connection, Table table) {
        long rowCount;
        String query = String.format("SELECT COUNT(*) FROM %s.%s",
                quoteName(table.getSchemaName()), quoteName(table.getName()));

        try {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            rs.next();
            rowCount = rs.getLong(1);
            stmt.close();
            //LOG.debug(String.format("Table %s.%s row count: %d", table.getSchemaName(), table.getName(), rowCount));
        } catch (SQLException e) {
            LOG.error("unable to count rows: " + query, e);
            rowCount = -1;
        }

        return rowCount;
    }

    /**
     * Select random rows from the database.
     *
     * @param table the table metadata
     * @param limit the number of rows to return.
     * @return a row or rows.
     */
    public LinkedList<List<ColumnValue>> getRandomRows(Connection connection, Table table, int limit) {
        String tableName = table.getName();
        String schemaName = table.getSchemaName();
        List<String> keyColumns = table.getKeyColumns();
        String keyColumnNames = "";
        String sep;
        LinkedList<List<ColumnValue>> randomRows = null;

        // get the key columns
        if ((keyColumns != null) && (keyColumns.size() > 0)) {
            sep = "";
            for (String columnName : keyColumns) {
                keyColumnNames = String.format("%s%s %s", keyColumnNames, sep, quoteName(columnName));
                sep = ",";
            }
        } else {
            keyColumnNames = quoteName(defaultKeyColumn);
        }
        String query = selectRandomRows(keyColumnNames, quoteName(schemaName), quoteName(tableName), limit);

        ResultSet rs;
        try {
            Statement stmt = connection.createStatement();
            rs = stmt.executeQuery(query);
            randomRows = getRowValues(rs);
            if ((randomRows == null) || (randomRows.size() == 0)) {
                LOG.info("failed to allocate random rows for table: {}", tableName);
            }
            rs.close();
            LOG.trace(String.format("got random rows for table %s.%s", schemaName, tableName));
        } catch (SQLException e) {
            LOG.error("unable to get random rows: " + query, e);
        }


        return randomRows;
    }


    /**
     * Insert a row into the target database.
     *
     * @param connection       the database connection to use.
     * @param table            the table metadata.
     * @param batchTransaction a batch transaction to work with
     * @param values           a List of values to assign.
     */
    @SuppressWarnings("unused")
    public void insert(Connection connection, Table table,
                       BatchTransaction batchTransaction, List<String> values) throws InterruptedException {
        String tableName = table.getName();
        String schemaName = table.getSchemaName();
        List<Column> columns = table.getColumns();

        String sep = "";
        String columnNames = "";
        String columnValues = "";
        int index = 0;
        for (Column column : columns) {
            columnNames = String.format("%s%s %s", columnNames, sep, quoteName(column.getName()));
            if (values != null) {
                columnValues = String.format("%s%s %s", columnValues, sep, values.get(index));
            } else {
                columnValues = String.format("%s%s '%s'", columnValues, sep, column.nextValue());
            }
            sep = ",";
            index++;
        }
        // now set the row randomizer value
        columnNames = String.format("%s%s %s", columnNames, sep, quoteName(dbRandom));
        columnValues = String.format("%s%s '%s'", columnValues, sep, unsignedInteger.nextValue());


        String query = String.format("INSERT INTO %s.%s (%s) VALUES(%s)",
                quoteName(schemaName), quoteName(tableName), columnNames, columnValues);
        try {
            batchTransaction.addStatement(query);
            table.getTableStats().incrementInsertAccumulator();
        } catch (SQLException e) {
            table.getTableStats().incrementErrorAccumulator();
            LOG.error("error adding query to the batch: {}", e.getMessage());
            LOG.error("query statement: {}", query);
        }
    }

    /**
     * Update a row into the target database.
     *
     * @param connection       the database connection to use.
     * @param table            the table metadata.
     * @param batchTransaction a batch transaction to work with.
     */
    public void update(Connection connection, Table table,
                       BatchTransaction batchTransaction) throws InterruptedException {
        String tableName = table.getName();
        String schemaName = table.getSchemaName();
        List<ColumnValue> keyColumns = table.getNextRandomRow(connection);
        List<String> updateColumnNames = table.getTableConfig().getUpdateColumnNames();

        String sep = "";
        String setClause = "";

        if ((updateColumnNames == null) || (updateColumnNames.size() == 0)) {
            LOG.error("table {} does not have update columns specified. Aborting operation", tableName);
            return;
        }

        for (String columnName : updateColumnNames) {
            String columnValue = table.getColumnByName(columnName).nextValue();
            setClause = String.format("%s%s %s = '%s'", setClause, sep, quoteName(columnName), columnValue);
            sep = ",";
        }
        // now update the randomizer column
        setClause = String.format("%s%s %s = '%s'", setClause, sep, quoteName(dbRandom), unsignedInteger.nextValue());


        sep = "";
        String whereClause = "";
        for (ColumnValue keyColumn : keyColumns) {
            whereClause = String.format("%s%s %s = '%s'",
                    whereClause, sep, quoteName(keyColumn.getColumnName()), keyColumn.getValue());
            sep = " AND ";
        }
        String query = String.format("UPDATE %s.%s SET %s WHERE %s",
                quoteName(schemaName), quoteName(tableName), setClause, whereClause);
        try {
            batchTransaction.addStatement(query);
            table.getTableStats().incrementUpdateAccumulator();
        } catch (SQLException e) {
            table.getTableStats().incrementErrorAccumulator();
            LOG.error("error adding query to the batch: {}", e.getMessage());
            LOG.error("query statement: {}", query);
        }
    }

    /**
     * Delete a row from the target database.
     *
     * @param connection       the database connection to use.
     * @param table            the table metadata.
     * @param batchTransaction a batch transaction to work with.
     */
    public void delete(Connection connection, Table table,
                       BatchTransaction batchTransaction) throws InterruptedException {
        String tableName = table.getName();
        String schemaName = table.getSchemaName();
        List<ColumnValue> keyColumns = table.getNextRandomRow(connection);

        String sep = "";
        String whereClause = "";
        for (ColumnValue keyColumn : keyColumns) {
            whereClause = String.format("%s%s %s = '%s'",
                    whereClause, sep, quoteName(keyColumn.getColumnName()), keyColumn.getValue());
            sep = " AND ";
        }
        String query = String.format("DELETE FROM %s.%s WHERE %s",
                quoteName(schemaName), quoteName(tableName), whereClause);
        try {
            batchTransaction.addStatement(query);
            table.getTableStats().incrementDeleteAccumulator();
        } catch (SQLException e) {
            table.getTableStats().incrementErrorAccumulator();
            LOG.error("error adding query to the batch: {}", e.getMessage());
            LOG.error("query statement: {}", query);
        }
    }

    /* Abstract Methods Follow */


    /**
     * Create a column that is an auto incrementing primary key for this table.
     *
     * @param columnName the name of the ID column
     * @return the column definition.
     */
    public abstract String autoIncrementPrimaryKey(String columnName);

    /**
     * Format a "select random rows" query for this database.
     *
     * @param keyColumnNames a comma separated list of key columns to select
     * @param schemaName     the name of the schema
     * @param tableName      the name of the table
     * @param limit          the number of rows to select.
     * @return the formatted query string.
     */
    public abstract String selectRandomRows(String keyColumnNames, String schemaName, String tableName, int limit);
    // https://beginnersbook.com/2018/11/sql-select-random-rows-from-table/

    /**
     * Override default column type mappings with mappings specific to this
     * database dialect.
     * @param dataTypeMapper a class that contains column type mappings
     */
    public abstract void overrideColumnTypes(DataTypeMapper dataTypeMapper);
}
