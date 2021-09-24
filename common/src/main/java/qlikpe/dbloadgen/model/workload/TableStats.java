package qlikpe.dbloadgen.model.workload;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import qlikpe.dbloadgen.model.database.Table;

import java.util.concurrent.atomic.LongAdder;

public class TableStats {
    private static final Logger LOG = LogManager.getLogger(Table.class);

    private LongAdder insertAdder;
    private LongAdder updateAdder;
    private LongAdder deleteAdder;
    private LongAdder errorAdder;
    private LongAdder preloadCount;
    private LongAdder preloadErrors;
    private LongAdder totalOperations;
    private String schemaName;
    private String tableName;

    public TableStats() {
        insertAdder = new LongAdder();
        updateAdder = new LongAdder();
        deleteAdder = new LongAdder();
        errorAdder = new LongAdder();
        totalOperations = new LongAdder();
        preloadCount = insertAdder;
        preloadErrors = errorAdder;
    }

    public void initMetaData(String schemaName, String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }
    /**
     * Bump the insert accumulator.
     */
    public void incrementInsertAccumulator() { insertAdder.increment(); totalOperations.increment(); }
    /**
     * Bump the update accumulator.
     */
    public void incrementUpdateAccumulator() { updateAdder.increment(); totalOperations.increment(); }
    /**
     * Bump the delete accumulator.
     */
    public void incrementDeleteAccumulator() { deleteAdder.increment(); totalOperations.increment(); }
    /**
     * Bump the error accumulator.
     */
    public void incrementErrorAccumulator() { errorAdder.increment(); }

    public void savePreloadStats() {
        long preloadInsertCount = preloadCount.sum();
        long preloadErrorCount = preloadErrors.sum();
        insertAdder = new LongAdder();
        errorAdder =new LongAdder();
        totalOperations.reset(); // only want the total to include runtime operations
        LOG.debug(String.format("Table %s.%s: Preload: inserts: %d  errors: %d",
                schemaName, tableName, preloadInsertCount, preloadErrorCount));
    }

    public void logStats() {
        long preloadInsertCount = preloadCount.sum();
        long preloadErrorCount = preloadErrors.sum();
        long insertCount = insertAdder.sum();
        long updateCount = updateAdder.sum();
        long deleteCount = deleteAdder.sum();
        long errorCount = errorAdder.sum();

        LOG.info(String.format("************ Stats for Table %s.%s *************", schemaName, tableName));

        LOG.info(String.format("Preload: inserts: %d  errors: %d", preloadInsertCount, preloadErrorCount));
        LOG.info(String.format("Runtime: inserts: %d  updates: %d deletes: %d errors %d",
                insertCount, updateCount, deleteCount, errorCount));
        LOG.info(String.format("         Total successful operations: %d",
                insertCount + updateCount + deleteCount));

        LOG.info("************ End of Table Stats *************");
    }

    public long getPreloadInserts() { return preloadCount.sum(); }
    public long getPreloadErrors() { return preloadErrors.sum(); }
    public long getInsertCount() { return insertAdder.sum(); }
    public long getUpdateCount() { return updateAdder.sum(); }
    public long getDeleteCount() { return deleteAdder.sum(); }
    public long getTotalOperations() { return totalOperations.sum(); }
    public long getErrorCount() { return errorAdder.sum(); }
}
