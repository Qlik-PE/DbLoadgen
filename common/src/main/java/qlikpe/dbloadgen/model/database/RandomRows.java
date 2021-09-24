package qlikpe.dbloadgen.model.database;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.util.LinkedList;
import java.util.List;

import static java.lang.Integer.min;

public class RandomRows {
    private final static Logger LOG = LogManager.getLogger(RandomRows.class);
    private final Table table;
    private LinkedList<List<ColumnValue>> randomRows;

    public RandomRows(Table table) {
        this.table = table;
        randomRows = new LinkedList<>();
    }

    /**
     * Manages a queue of random rows selected from the database for use
     * in update and delete operations. We request multiple random rows
     * at once for the sake of efficiency. It would be very expensive to
     * select a random row for each update and delete operation.
     * @param connection the database connection to use.
     *
     * @return a List of ColumnValues
     */
    public synchronized List<ColumnValue> getNextRandomRow(Connection connection) {
        int limit = 100;
        if (randomRows.size() == 0) {
            randomRows = table.getDatabase().getRandomRows(connection, table, limit);
            if (randomRows.size() ==  0) {

                long count = table.getDatabase().countRows(connection, table);
                int numRows = 3*limit;
                if (count < numRows) {
                    LOG.info("all rows depleted for table {} limit {}. Replenishing {} rows.",
                            table.getName(), limit, numRows);

                    Database database = table.getDatabase();
                    BatchTransaction batchTransaction = new BatchTransaction(connection, numRows);
                    for (int i = 0; i < numRows; i++) {
                        try {
                            database.insert(connection, table, batchTransaction, null);
                        } catch (InterruptedException e) {
                            LOG.warn("getNextRandomRow: interrupted exception: {}", e.getMessage());
                        }
                    }
                    batchTransaction.executeBatch();  // flush any uncommitted operations.
                } else {
                    LOG.warn("failed to refill the random row pool for table {} limit {}. Retrying.",
                            table.getName(), limit);
                }

                randomRows = table.getDatabase().getRandomRows(connection, table, limit);
                if (randomRows.size() ==  0) {
                    // something went wrong
                    LOG.error("failed to refill the random row pool for table {}.", table.getName());
                }
            }
        }
        return randomRows.removeFirst();
    }
}
