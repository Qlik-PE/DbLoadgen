package qlikpe.dbloadgen.model.workload;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import qlikpe.dbloadgen.model.database.BatchTransaction;
import qlikpe.dbloadgen.model.database.Database;
import qlikpe.dbloadgen.model.database.DatabaseConnection;
import qlikpe.dbloadgen.model.database.Table;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;


/**
 * An independent thread that drives the runtime workload in the database.
 */
public class WorkerThread implements Runnable {
    private final static Logger LOG = LogManager.getLogger(WorkerThread.class);
    private final String threadName;
    private final CountDownLatch latch;
    private final Database database;
    private final BatchTransaction batchTransaction;
    private final Connection connection;
    private final List<Table> tables;
    private final int numTables;
    private final int threadSleep;  // delay between operations

    /**
     * Construct the worker thread.
     * @param threadName a unique name for this thread.
     * @param latch a countdown latch used by the parent thread to wait.
     * @param workloadConfig workload configuration properties.
     * @param databaseType the type of database we are working with.
     * @param connectionInfo the configuration properties needed to connect to the database.
     * @param tables the tables that are a part of this workload.
     */
    public WorkerThread(String threadName, CountDownLatch latch, WorkloadConfig workloadConfig, String databaseType,
                        Properties connectionInfo, List<Table> tables) {
        this.threadName = threadName;
        this.latch = latch;

        this.threadSleep = workloadConfig.getWorkerThreadSleep();
        this.database = Database.databaseFactory(databaseType);

        DatabaseConnection databaseConnection = new DatabaseConnection(databaseType, connectionInfo);
        this.connection = databaseConnection.connect();

        this.batchTransaction = new BatchTransaction(connection,
                workloadConfig.getWorkerBatchSize(), workloadConfig.getWorkerBatchSleep());


        // need our own copy of each table for concurrency reasons.
        this.tables = new ArrayList<>();
        for(Table table : tables) {
            TableConfig tableConfig = table.getTableConfig();
            tableConfig.setParent(table);
            if (tableConfig.getUpdateColumnNames() != null) {
                // only add tables with update column names specified.
                // null implies "do not do CDC on this table"
                this.tables.add(new Table(table, database));
            } else {
                LOG.warn("not configuring table {}.{} for CDC", table.getSchemaName(), table.getName());
            }
        }
        this.numTables = this.tables.size();
    }

    /**
     * Randomly select a table to use for this operation.
     * @return the selected table.
     */
    public Table getRandomTable() {
        return tables.get(ThreadLocalRandom.current().nextInt(0, numTables));
    }

    /**
     * Return a randomly selected operation based on a distribution of insert, update,
     * and delete operations.
     * @param table the table we are generating an operation for.
     * @return a database operation type.
     */
    public Database.OperationType getRandomOperation(Table table) {
        Database.OperationType operationType;

        int insertPct = table.getTableConfig().getOperationPct().getInsert();
        int updatePct = table.getTableConfig().getOperationPct().getUpdate();
        int deletePct = table.getTableConfig().getOperationPct().getDelete();
        int totalPct = insertPct + updatePct + deletePct;

        int value = ThreadLocalRandom.current().nextInt(0, totalPct);
        if (value < insertPct)
            operationType = Database.OperationType.INSERT;
        else if (value < (insertPct + updatePct))
            operationType = Database.OperationType.UPDATE;
        else operationType = Database.OperationType.DELETE;
        return operationType;
    }

    @Override
    public void run() {
        boolean stopFlag = false;
        LOG.info("Starting worker thread: {}", threadName);
        while (!stopFlag) {
            try {
                String operation;
                Table table = getRandomTable();
                Database.OperationType operationType = getRandomOperation(table);
                switch(operationType) {
                    case INSERT:
                        operation = "INSERT";
                        database.insert(connection, table, batchTransaction, null);
                        break;
                    case UPDATE:
                        operation = "UPDATE";
                        database.update(connection, table, batchTransaction);
                        break;
                    default:
                        operation = "DELETE";
                        database.delete(connection, table, batchTransaction);
                        break;
                }
                LOG.trace("Thread: {}, Table: {}, Operation: {}", threadName, table.getName(), operation);
                if (threadSleep > 0) {
                    // slow things down for testing.
                    Thread.sleep(threadSleep);
                }
            } catch (InterruptedException e) {
                stopFlag = true;
                LOG.debug("Worker: {} caught an interrupted exception", threadName);
            } catch (Exception e) {
                stopFlag = true;
                LOG.error("Worker: {} caught unexpected exception", threadName, e);
            }
        }
        LOG.debug("decrementing latch in worker thread: {}, latch count: {}", threadName, latch.getCount());
        latch.countDown();
        LOG.info("Exiting worker thread: {} latch count: {}", threadName, latch.getCount());
    }
}
