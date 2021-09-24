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

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Integer.min;


/**
 * This class creates and executes a batch transaction.
 * It accumulates SQL statements until it is told to execute the
 * transaction. It cleans up after executing the transaction and
 * may be reused rather than creating a new instance for a new batch.
 */
public class BatchTransaction {
    private static final Logger LOG = LogManager.getLogger(BatchTransaction.class);
    private final Connection connection;
    private final int batchSize;
    private final int batchSleep;
    private int size = 0;
    private Statement stmt;
    private final List<String> sqlStatements;

    /**
     * Create a new batch transaction.
     * @param connection the database connection to use.
     * @param batchSize the number of operations in each batch.
     */
    public BatchTransaction(Connection connection, int batchSize) {
        this.connection = connection;
        this.batchSize = batchSize;
        this.batchSleep = 0;
        this.sqlStatements = new ArrayList<>();
    }

    /**
     * Create a new batch transaction.
     * @param connection the database connection to use.
     * @param batchSize the number of operations in each batch.
     * @param batchSleep the number of milliseconds to sleep between batches.
     */
    public BatchTransaction(Connection connection, int batchSize, int batchSleep) {
        this.connection = connection;
        this.batchSize = batchSize;
        this.batchSleep = batchSleep;
        this.sqlStatements = new ArrayList<>();
    }

    /**
     * Start a new batch.
     * @throws SQLException if we run into an error.
     */
    private void startBatch() throws SQLException {
        connection.setAutoCommit(false);
        stmt = connection.createStatement();
        sqlStatements.clear();
    }

    /**
     * Add a new SQL statement to the batch, executing the batch
     * if the number of queued statements equals the batchSize.
     * @param sqlStatement a properly formatted SQL statement as a string.
     * @throws SQLException if we have an issue.
     * @throws InterruptedException if our sleep gets interrupted.
     */
    public void addStatement(String sqlStatement) throws SQLException, InterruptedException{
        if (size == 0)
            startBatch();

        sqlStatements.add(sqlStatement);  // saving these off for error handling.
        stmt.addBatch(sqlStatement);
        size++;

        if (size >= batchSize) {
            executeBatch();
            if (batchSleep > 0) {
                Thread.sleep(batchSleep);
            }
        }
    }

    /**
     * Execute the current batch transaction. It does nothing if
     * there is nothing in the batch to commit.
     */
    public void executeBatch() {
        if (size > 0) {
            try {
                stmt.executeBatch();
                connection.commit();
            } catch (SQLException e) {
                String exceptionName = e.getClass().getName();
                String message = String.format("%s: %s", exceptionName, e.getMessage());
                if ((e instanceof BatchUpdateException) || (e instanceof SQLTransactionRollbackException)) {
                    LOG.warn(message);
                } else {
                    LOG.error("Unexpected exception: " + message);
                }
                oneByOne();  // attempt to replay operations one at a time.
            }

            try {
                stmt.close();
            } catch (SQLException e) {
                LOG.warn("executeBatch(): Failed to close statement: {}}", e.getMessage());
            }

            size = 0;
        }
    }

    /**
     * Replay a failed transaction one operation at a time.
     */
    private void oneByOne() {
        LOG.info("entering one-by-one mode");
        Statement singleStatement = null;

        try {
            connection.rollback(); // rollback the batch transaction
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            LOG.warn("oneByOne: rollback issue: {}", e.getMessage());
        }
        for(String query : sqlStatements) {
            String queryTrunc;
            if (query.contains("INSERT"))
                queryTrunc = query.substring(0, query.indexOf('('));
            else queryTrunc = query;
            try {

                //connection.setAutoCommit(true);
                singleStatement = connection.createStatement();
                singleStatement.execute(query);
                LOG.debug("one-by-one operation succeeded: "+ queryTrunc);
            } catch(SQLException e) {
                if (e.getMessage().contains("Duplicate entry")) {
                    LOG.warn(String.format("one-by-one operation failed: %s message: %s", queryTrunc, e.getMessage()));
                } else {
                    LOG.warn(String.format("one-by-one operation failed: %s message: %s", query, e.getMessage()));
                }
            }
            try {
                singleStatement.close();
            } catch (SQLException e) {
                LOG.warn("oneByOne(): Failed to close statement: {}}", e.getMessage());
            }
        }
        sqlStatements.clear();
    }
}