package qlikpe.dbloadgen.model.workload;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import qlikpe.dbloadgen.model.database.BatchTransaction;
import qlikpe.dbloadgen.model.database.Database;
import qlikpe.dbloadgen.model.database.DatabaseConnection;
import qlikpe.dbloadgen.model.database.Table;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


/**
 * A thread that loads a table into the database during the preload phase.
 */
public class PreloadThread implements Runnable {
    private final static Logger LOG = LogManager.getLogger(PreloadThread.class);
    private final String threadName;
    private final CountDownLatch latch;
    private final Database database;
    private final BatchTransaction batchTransaction;
    private final Connection connection;
    private final Table table;
    private final WorkloadConfig workloadConfig;

    /**
     * Construct the worker thread.
     * @param threadName a unique name for this thread.
     * @param latch a countdown latch used by the parent thread to wait.
     * @param workloadConfig the workload configuration
     * @param databaseType the type of database we are working with.
     * @param connectionInfo the configuration properties needed to connect to the database.
     * @param table the tables that are a part of this workload.
     */
    public PreloadThread(String threadName, CountDownLatch latch, WorkloadConfig workloadConfig, String databaseType,
                         Properties connectionInfo, Table table) {

        this.threadName = threadName;
        this.latch = latch;
        this.table = table;
        this.workloadConfig = workloadConfig;

        this.database = Database.databaseFactory(databaseType);
        DatabaseConnection databaseConnection = new DatabaseConnection(databaseType, connectionInfo);
        this.connection = databaseConnection.connect();
        this.batchTransaction = new BatchTransaction(connection, workloadConfig.getPreloadBatchSize());
    }

    /**
     * Look at the configuration and determine if we will use canned data or
     * will generate column values during the preload process.
     * @param tableName the name of the table.
     * @return the number of rows to generate, -1 to used the table's canned data.
     */
    private int getPreloadForTable(String tableName) {
        int count;
        String preload = null;
        for(TableConfig tableConfig : workloadConfig.getTables()) {
            if (tableConfig.getName().equals(tableName)) {
                preload = tableConfig.getPreload();
                break;
            }
        }

        if (preload == null) {
            LOG.error("preload not set for table: {}. Defaulting to generating 1000 rows.", tableName);
            count = 1000;
        } else if (preload.equalsIgnoreCase("data"))
            count = -1;
        else count = Integer.parseInt(preload);

        return count;
    }

    @Override
    public void run() {
        LOG.debug("Starting preload thread: {}", threadName);

        try {
            String tableName = table.getName();
            long rowCount = database.countRows(connection, table);
            if (rowCount == 0) {
                // only pre-load if the tables are empty.
                int preloadCount = getPreloadForTable(tableName);
                if (preloadCount == -1) {
                    List<String> data = table.getData();
                    if ((data != null) && (data.size() > 0)) {
                        for (String row : data) {
                            List<String> values = Arrays.asList(row.split(","));
                            database.insert(connection, table, batchTransaction, values);
                        }
                    } else {
                        LOG.info("data section was empty for table {}.{}", table.getSchemaName(), tableName);
                    }
                } else {
                    for (int i = 0; i < preloadCount; i++) {
                        database.insert(connection, table, batchTransaction, null);
                    }
                }

                // flush any remaining preload operations.
                batchTransaction.executeBatch();
                table.setPreloadCount((int)database.countRows(connection, table));
            } else {
                table.setPreloadCount((int)rowCount);
                LOG.debug("skipping preload for table {}. It was already initialized with {} rows",
                        tableName, rowCount);
            }
            table.getTableStats().savePreloadStats();
        } catch (InterruptedException e) {
            LOG.debug("Preload: {} caught interrupted exception", threadName);
        }

        latch.countDown();
        LOG.debug("Exiting worker thread: {}", threadName);
    }
}
