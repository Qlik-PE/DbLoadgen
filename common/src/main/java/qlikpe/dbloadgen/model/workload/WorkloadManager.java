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
package qlikpe.dbloadgen.model.workload;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import qlikpe.dbloadgen.DbLoadgenProperties;
import qlikpe.dbloadgen.model.database.*;
import qlikpe.dbloadgen.model.output.OutputBuffer;
import qlikpe.dbloadgen.model.output.OutputBufferMap;
import qlikpe.dbloadgen.model.output.TableBuffer;
import qlikpe.dbloadgen.model.output.TextBuffer;

import java.io.*;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Math.round;

@Getter
public class WorkloadManager {
    private final static Logger LOG = LogManager.getLogger(WorkloadManager.class);

    private final DbLoadgenProperties dbLoadgenProperties;
    private final OutputBufferMap outputBufferMap;
    private final int latchTimeout = 30;
    private DatabaseConnection databaseConnection;
    private Database database;
    private String connectionName;
    private String runtimeConfig;
    private final List<Table> tables;
    private ExecutorService executor;
    private WorkloadConfig workloadConfig;
    private Properties connectionInfo;
    private int batchSize;
    private int batchSleep;
    private int threadSleep;
    private int workers;
    private final AtomicInteger tableMetadataParsed = new AtomicInteger();
    private int preloadedTables = 0;
    private int cdcElapsedPct = 0;
    private int schemaInitPct = 0;
    private String databaseType;
    private DataTypeMapper dataTypeMapper;
    private CountDownLatch countdownLatch;
    private boolean preloadRunning = false;
    private boolean cdcTestRunning = false;
    private boolean parsingMetadata = false;
    private boolean initializingSchema = false;
    private final AtomicBoolean executingCommand = new AtomicBoolean(false);
    private boolean stopThreads = false;

    /**
     * Constructor that does not initialize runtime properties
     * at construction time.
     */
    public WorkloadManager() {
        dbLoadgenProperties = DbLoadgenProperties.getInstance();
        outputBufferMap = OutputBufferMap.getInstance();
        tables = Collections.synchronizedList(new ArrayList<>());
    }

    /**
     * Constructor that initializes runtime properties.
     *
     * @param runtimeProperties runtime properties that will override defaults.
     */
    public WorkloadManager(Properties runtimeProperties) {
        dbLoadgenProperties = DbLoadgenProperties.getInstance();
        outputBufferMap = OutputBufferMap.getInstance();
        setPropertyManager(runtimeProperties);
        tables = Collections.synchronizedList(new ArrayList<>());
    }

    /**
     * Set the workload properties.
     * @param runtimeProperties the properties we want to set.
     */
    public void setPropertyManager(Properties runtimeProperties) {
        dbLoadgenProperties.setProperties(runtimeProperties);
    }

    /**
     * Save the application context for next time.
     */
    public void saveContext() {
       dbLoadgenProperties.saveContext();
    }

    /**
     * Execute a workload command.
     * @param command the command we want to execute.
     * @param async true causes preload and run tasks to execute asynchronously.
     * @throws WorkloadInitializationException on an error.
     */
    public void executeCommand(String command, boolean async) throws WorkloadInitializationException {

        // clean up from prior runs
        tables.clear();
        outputBufferMap.clear();

        executingCommand.set(true);
        readConnectionInfo();
        readWorkloadInfo();

        AtomicBoolean asyncWait = new AtomicBoolean(!async);
        // execute the command in a separate thread.
        Thread asyncThread = new Thread(() -> {
            switch(command) {
                case "test-connection":
                    asyncWait.set(true);  // always run synchronously
                    LOG.info("testing connectivity");
                    testConnection();
                    break;
                case "cleanup":
                    LOG.info("cleaning up from prior runs");
                    asyncWait.set(true);  // always run synchronously
                    if (readTableInfo()) {
                        cleanup();
                    }
                    break;
                case "init":
                    LOG.info("initializing schema");
                    if (readTableInfo()) {
                        initSchema();
                    }
                    break;
                case "preload":
                    if (readTableInfo()) {
                        LOG.info("preloading tables");
                        getPreloadStats();
                        preloadTables();
                        logPreloadStats();
                    }
                    break;
                case "run":
                    if (readTableInfo()) {
                        LOG.info("running cdc test");
                        getRuntimeStats();
                        cdcTest();
                        logRuntimeStats();
                    }
                    break;
                case "reset":
                    if (readTableInfo()) {
                        LOG.info("cleaning up from prior runs");
                        if (cleanup()) {
                            LOG.info("initializing schema");
                            if (initSchema()) {
                                LOG.info("preloading tables");
                                getPreloadStats();
                                preloadTables();
                                logPreloadStats();
                            }
                        }
                    }
                    break;
                case "end-to-end":
                    if (readTableInfo()) {
                        LOG.info("cleaning up from prior runs");
                        if (cleanup()) {
                            LOG.info("initializing schema");
                            if (initSchema()) {
                                getPreloadStats();
                                getRuntimeStats();
                                LOG.info("preloading tables");
                                preloadTables();
                                logPreloadStats();
                                LOG.info("running test");
                                cdcTest();
                                logRuntimeStats();
                            }
                        }
                    }
                    break;
                case "list-workloads":
                    asyncWait.set(true);  // always run synchronously
                    System.out.println("Available Workloads:");
                    for(String workloadName : WorkloadConfigList.getInstance().getWorkloadNames()) {
                        System.out.println("   " + workloadName);
                    }
                    break;
                case "list-connections":
                    asyncWait.set(true);  // always run synchronously
                    System.out.println("Available Connections:");
                    for(String connectionName : dbLoadgenProperties.getConnectionList().getConnectionNames()) {
                        System.out.println("   " + connectionName);
                    }
                    break;
                default:
                    asyncWait.set(true);  // always run synchronously
                    LOG.error("Unrecognized command: " + command);
            }
            executingCommand.set(false);
        });
        asyncThread.start();
        // if execution is synchronous, wait for the thread to complete.
        if (asyncWait.get()) {
            try {
                asyncThread.join();
            } catch (InterruptedException e) {
                LOG.info("{}: thread join interrupted: {}", command, e.getMessage());
            }
        }
        LOG.info("Execution complete");
    }

    /**
     * Ensures we can connect to the database.
     */
    public void testConnection() {
        databaseConnection.testConnection();
    }

    /**
     * Initialize the target database schema.
     * @return true on success, false if there were issues.
     */
    public boolean initSchema() {
        boolean rval;
        TextBuffer outputBuffer = outputBufferMap.getTextBuffer(OutputBufferMap.INITIALIZE_SCHEMA,
                "Schema Initialization");
        outputBuffer.resetBuffer();

        initializingSchema = true;
        Connection connection = databaseConnection.connect();
        if (connection != null) {
            String schemaName = workloadConfig.getSchema();
            LOG.debug("initSchema(): creating schema {}", schemaName);
            if (database.createSchema(connection, schemaName)) {

                int failures = 0;
                int completed = 0;
                if (tables.size() == 0)
                    LOG.error("initSchema(): No tables defined to initialize");
                else LOG.debug("initSchema: initializing {} tables", tables.size());
                for (Table table : tables) {
                    if (stopThreads) {
                        LOG.debug("initSchema(): stopThreads detected. Exiting.");
                        initializingSchema =  false;
                        return false;
                    }
                    LOG.debug("initSchema(): initializing table {}", table.getName());
                    if (!database.createTable(connection, table))
                        failures++;
                    database.addRandomizer(connection, table);
                    completed++;
                    schemaInitPct = round((completed * 100.0f)/tables.size());
                }
                if (failures == 0) {
                    rval = true;
                    outputBuffer.addLine(OutputBuffer.Priority.SUCCESS, "schema initialization complete");
                } else {
                    rval = false;
                    outputBuffer.addLine(OutputBuffer.Priority.WARNING, "schema initialization completed with errors");
                }
            } else {
                LOG.error("initSchema(): create schema failed");
                rval = false;
            }
        } else {
            rval = false;
            String message = "Could not connect to the target database";
            outputBuffer.addLine(OutputBuffer.Priority.ERROR, message);
            LOG.error(message);
        }
        initializingSchema = false;
        return rval;
    }

    /**
     * Cleanup any remnants from a prior run.
     * @return true on success, false if there were issues.
     */
    public boolean cleanup() {
        boolean rval;
        TextBuffer outputBuffer = outputBufferMap.getTextBuffer(OutputBufferMap.CLEANUP,
                "Database Cleanup Results");
        outputBuffer.resetBuffer();

        Connection connection = databaseConnection.connect();
        if (connection != null) {
            int errorCount = 0;
            String schemaName = workloadConfig.getSchema();

            for (Table table : tables) {
                if (stopThreads) {
                    LOG.debug("cleanup(): stopThreads detected. Exiting.");
                    return false;
                }

                if (!database.dropTable(connection, table))
                    errorCount++;
            }
            if (!database.dropSchema(connection, schemaName))
                errorCount++;

            if (errorCount == 0) {
                rval = true;
                outputBuffer.addLine(OutputBuffer.Priority.SUCCESS, "schema cleanup complete");
            } else {
                rval = false;
                outputBuffer.addLine(OutputBuffer.Priority.WARNING, "schema cleanup completed with errors");
            }
        } else {
            rval = false;
            outputBuffer.addLine(OutputBuffer.Priority.ERROR, "Failed to connect to the database");
            LOG.error("could not connect to the target database");
        }
        return rval;
    }

    /**
     * Preload the target schema prior to running the test.
     * This method first checks to be sure that the schema has
     * been initialized.
     */
    public void preloadTables() {
        int preloadWorkers = workloadConfig.getPreloadThreads();
        int numTables = tables.size();
        preloadedTables = 0;

        preloadRunning = true;
        stopThreads = false;
        countdownLatch = new CountDownLatch(numTables);
        executor = Executors.newFixedThreadPool(preloadWorkers);
        for (Table table : tables) {
            if (stopThreads)
                break;
            String threadName = String.format("preload-%s", table.getName());
            executor.execute(new Thread(new PreloadThread(threadName, countdownLatch, workloadConfig,
                    databaseType, connectionInfo, table)));
        }
        executor.shutdown();  // we are done adding threads
        // track thread completion stats
        while(countdownLatch.getCount() > 0) {
            if (stopThreads)
                break;
            preloadedTables = numTables - (int)countdownLatch.getCount();
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                LOG.info("preloadTables() interrupted exception");
            }
        }
        // now wait for all threads to exit.
        try {
            LOG.debug("preloadTables(): waiting for preload countdown latch. Count {}", countdownLatch.getCount());
            if (!countdownLatch.await(latchTimeout, TimeUnit.SECONDS)){
                LOG.warn("preloadTables(): countdownLatch.await() expired before threads ended: count: {}",
                        countdownLatch.getCount());
            } else LOG.debug("preloadTables(): table load countdown latch await: threads finished");
            preloadedTables = numTables;
        } catch (InterruptedException ex) {
            LOG.warn("preloadTables(): interrupt received during latch wait", ex);
        }
        stopThreads = false;
        preloadRunning = false;
    }

    public int getPreloadPct() {
        return round((preloadedTables * 100.0f)/tables.size());
    }

    public int getParsingPct() {
        return round((tableMetadataParsed.get() * 100.0f)/workloadConfig.getTables().size());
    }

    /**
     * Run the test. It will first initialize the database
     * if the database has not already been set up. This allows
     * this function to be called whether or not the initialization
     * workflow has been performed.
     */
    public void cdcTest() {
        int currentWorker;
        long duration = (long) workloadConfig.getDuration() * 60 * 1000;

        LOG.info("cdcTest(): workers will run for {} minutes", workloadConfig.getDuration());

        List<List<Table>> workerTables = distributeTables();

        stopThreads = false;
        cdcTestRunning = true;
        countdownLatch = new CountDownLatch(workers);
        executor = Executors.newCachedThreadPool();
        for (currentWorker = 0; currentWorker < workers; currentWorker++) {
            if (stopThreads)
                break;
            String threadName = String.format("worker-%d", currentWorker);
            Thread t = new Thread(new WorkerThread(threadName, countdownLatch,
                    workloadConfig, databaseType, connectionInfo, workerTables.get(currentWorker)));
            //Timer timer = new Timer(threadName);
            //timer.schedule(new WorkerTimeoutTask(threadName, t, timer), duration);
            executor.submit(t);
        }
        executor.shutdown();

        try {
            long cdcStartTime = System.currentTimeMillis();
            long cdcElapsedTime = 0;
            LOG.debug("cdcTest: entering duration loop: elapsedTime: {} duration: {}", cdcElapsedTime, duration);
            while(cdcElapsedTime < duration) {
                if (stopThreads) {
                    break;
                }
                cdcElapsedTime = System.currentTimeMillis() - cdcStartTime;
                cdcElapsedPct = round((cdcElapsedTime * 100.0f)/duration);
                Thread.sleep(1000);
            }
            stopThreads();
            LOG.debug("cdcTest: leaving duration loop");

            // now wait for all threads to exit.
            if (!countdownLatch.await(latchTimeout, TimeUnit.SECONDS)){
                LOG.warn("cdcTest(): countdownLatch.await() expired before threads ended: count: {}",
                        countdownLatch.getCount());
            } else LOG.debug("cdcTest(): table load countdown latch await: threads finished");
        } catch (InterruptedException ex) {
            LOG.warn("cdcTest(): interrupt received during runtime latch wait", ex);
        }
        stopThreads = false;
        cdcTestRunning = false;
        LOG.debug("cdcTest(): exiting");
    }

    /**
     * Send an interrupt to all preload or worker threads
     * that might be running.
     */
    public void stopThreads() {
        LOG.info("stopThreads(): sending stop to all threads");
        stopThreads = true;
        if (executor != null) {
            List<Runnable> pending = executor.shutdownNow();
            for(int i=0; i < pending.size(); i++) {
                // these threads were created, but not yet executed. Cleanup the latch.
                countdownLatch.countDown();
            }
            executor = null;
        }
    }

    /**
     * Allocate tables to each thread.
     * If the configuration parameter AllTablesAllThreads is true,
     * then allocate all tables to all threads. Otherwise, iterate
     * over the list of tables, allocating each table to only a single
     * thread.
     * @return a list of a list of tables to allocate to each thread.
     */
    public List<List<Table>> distributeTables() {
        int currentWorker;
        List<List<Table>> workerTables = new ArrayList<>();

        if (!getAllTablesAllThreads()) {
            // allocate each table to a single thread
            LOG.info("distributeTables(): allTablesAllThreads = false");
            for (currentWorker = 0; currentWorker < workers; currentWorker++)
                workerTables.add(new ArrayList<>());
            currentWorker = 0;
            for (Table table : tables) {
                workerTables.get(currentWorker).add(table);
                currentWorker++;
                if (currentWorker >= workers)
                    currentWorker = 0;
            }
        } else {
            // allocate all tables to all threads
            LOG.info("distributeTables(): allTablesAllThreads = true");
            for (currentWorker = 0; currentWorker < workers; currentWorker++)
                workerTables.add(tables);
        }

        return workerTables;
    }

    /**
     * Get the configuration parameter AllTablesAllThreads and convert
     * it from a String to a boolean.
     * @return the value as a boolean.
     */
    public boolean getAllTablesAllThreads() {
        String value = workloadConfig.getAllTablesAllThreads();
        boolean rval;
        if (value.equalsIgnoreCase("true"))
            rval = true;
        else if (value.equalsIgnoreCase("false"))
            rval = false;
        else {
            LOG.warn("workload AllTablesAllThreads had an invalid value. Defaulting to false");
            rval = false;
        }
        return rval;
    }

    /**
     * Override a default property with a runtime value.
     *
     * @param inputProps  the input properties
     * @param inputKey    the input property key
     * @param outputProps the output properties.
     * @param outputKey   the output property key
     */
    private void overrideProperty(Properties inputProps, String inputKey, Properties outputProps, String outputKey) {
        String value = inputProps.getProperty(inputKey);
        if (value != null)
            outputProps.setProperty(outputKey, value);
    }

    /**
     * Get the subset of properties that pertain to getting the database connection.
     *
     * @return the relevant properties.
     */
    private Properties getConnectionProperties(String connectionName) {
        Properties connectionProps = dbLoadgenProperties.getPropertySubset(DbLoadgenProperties.CONNECTION_PREFIX + connectionName,
                true);
        Properties inputProps = dbLoadgenProperties.getProperties();
        overrideProperty(inputProps, DbLoadgenProperties.DATABASE_TYPE, connectionProps, "databaseType");
        overrideProperty(inputProps, DbLoadgenProperties.JDBC_DRIVER, connectionProps, "jdbcDriver");
        overrideProperty(inputProps, DbLoadgenProperties.URL, connectionProps, "url");
        overrideProperty(inputProps, DbLoadgenProperties.USERNAME, connectionProps, "userName");
        overrideProperty(inputProps, DbLoadgenProperties.PASSWORD, connectionProps, "password");

        dbLoadgenProperties.printProperties("Connection Info", connectionProps);
        return connectionProps;
    }


    /**
     * Read the connection info from the properties file.
     *
     * @throws WorkloadInitializationException if there are problems with the connection info.
     */
    private void readConnectionInfo() throws WorkloadInitializationException {
        connectionName = dbLoadgenProperties.getProperty(DbLoadgenProperties.CONNECTION_NAME);
        if (connectionName == null) {
            String message = "connection name not specified";
            LOG.error(message);
            throw new WorkloadInitializationException(message);
        }

        connectionInfo = getConnectionProperties(connectionName);
        if (connectionInfo.isEmpty()) {
            String message = String.format("connection name %s did not map to connection properties", connectionName);
            LOG.error(message);
            throw new WorkloadInitializationException(message);
        }

        databaseType = connectionInfo.getProperty("databaseType");
        databaseConnection = new DatabaseConnection(databaseType, connectionInfo);
        database = Database.databaseFactory(databaseType);
        if (database == null)
            throw new WorkloadInitializationException(("invalid database type: " + databaseType));

        dataTypeMapper = new DataTypeMapper(database);
        // override the default types with database-specific types where needed.
        database.overrideColumnTypes(dataTypeMapper);
    }


    /**
     * Read the workload configuration info.
     *
     * @throws WorkloadInitializationException if the dataset cannot be configured.
     */
    private void readWorkloadInfo() throws WorkloadInitializationException {
        WorkloadConfigList workloadConfigList = WorkloadConfigList.getInstance();
        runtimeConfig = dbLoadgenProperties.getProperty(DbLoadgenProperties.WORKLOAD_CONFIG_FILE);
        if (runtimeConfig == null) {
            String message = "workload configuration file was not specified";
            LOG.error(message);
            throw new WorkloadInitializationException(message);
        }
        workloadConfig = workloadConfigList.getWorkloadConfigByName(runtimeConfig);
        if (workloadConfig == null) {
            String message = String.format("specified workload %s was not found", runtimeConfig);
            LOG.error(message);
            throw new WorkloadInitializationException(message);
        }

        batchSize = workloadConfig.getWorkerBatchSize();
        batchSleep = workloadConfig.getWorkerBatchSleep();
        threadSleep = workloadConfig.getWorkerThreadSleep();
        workers = workloadConfig.getWorkerThreads();
    }

    private boolean readTableInfo() {
        AtomicInteger errors = new AtomicInteger();
        countdownLatch = new CountDownLatch(workloadConfig.getTables().size());
        parsingMetadata = true;

        tableMetadataParsed.set(0);

        stopThreads = false;
        executor = Executors.newFixedThreadPool(4);
        for (TableConfig tableConfig : workloadConfig.getTables()) {
            if (stopThreads)
                break;

            executor.execute(new Thread(() -> {
                boolean rval = readTableMetadata(tableConfig);
                if (!rval) errors.getAndIncrement();
                tableMetadataParsed.getAndIncrement();
                countdownLatch.countDown();
            }));
        }
        executor.shutdown();
        try {
            if (!countdownLatch.await(latchTimeout, TimeUnit.SECONDS)){
                LOG.warn("readTableInfo(): countdownLatch.await() expired before threads ended: count: {}",
                        countdownLatch.getCount());
            } else LOG.debug("readTableInfo(): table load countdown latch await: threads finished");
        } catch (InterruptedException e) {
            LOG.info("readTableInfo(): table load countdown latch interrupted: {}", e.getMessage());
        }
        parsingMetadata = false;
        if (stopThreads) {
            stopThreads = false;
            return false;
        } else return errors.get() == 0;
    }

    /**
     * Ingest the metadata for the specified tables.
     */
    private boolean readTableMetadata(TableConfig tableConfig)  {
        boolean rval;
        String fileName;

        LOG.debug("readTableMetadata(): {}", tableConfig.getName());
        // now ingest the configurations for the specified tables.
        String datasetDir = dbLoadgenProperties.getProperty(DbLoadgenProperties.DATADIR);
        String resourceDir = DbLoadgenProperties.DATASET_RESOURCE_DIR;
        String schemaName = workloadConfig.getSchema();

        fileName = "unset";
        try {
            InputStream is;
            try {
                // look in the dataset directory.
                fileName = String.format("%s/%s/%s.yml", datasetDir, runtimeConfig, tableConfig.getName());
                is = getFileAsInputStream(fileName);
            } catch (FileNotFoundException e) {
                // that didn't work, so look in the classpath
                LOG.info("looking for dataset in classpath resources: {}", fileName);
                fileName = String.format("%s/%s/%s.yml", resourceDir, runtimeConfig, tableConfig.getName());
                is = getFileFromResourceAsStream(fileName);
            }
            Yaml yaml = new Yaml(new Constructor(Table.class));

            Table table = yaml.load(is);
            table.setTableConfig(tableConfig);
            table.setSchemaName(schemaName);
            table.setDatabase(database);
            table.getTableStats().initMetaData(schemaName, table.getName());
            table.setColumnTypes(dataTypeMapper);
            tables.add(table);
            rval = true;
        } catch (FileNotFoundException e) {
            String message = "could not open dataset table configuration: " + fileName;
            LOG.error(message, e);
            rval = false;
        }
        return rval;
    }

    /**
     * Get a file from the resources folder.
     *
     * @param fileName the name of the file.
     * @return an InputStream;
     */
    private InputStream getFileFromResourceAsStream(String fileName) throws FileNotFoundException {
        // The class loader that loaded the class
        ClassLoader classLoader = getClass().getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream(fileName);

        // the stream holding the file content
        if (inputStream == null) {
            throw new FileNotFoundException("file not found! " + fileName);
        }
        return inputStream;
    }

    /**
     * Get a file as an input stream.
     *
     * @param fileName the name of the file.
     * @return an InputStream;
     */
    private InputStream getFileAsInputStream(String fileName) throws FileNotFoundException {
        return new FileInputStream(fileName);
    }

    public TableBuffer getPreloadStats() {
        TableBuffer tableBuffer = outputBufferMap.getTableBuffer(OutputBufferMap.PRELOAD_STATS,
                "Preload Table Statistics");
        tableBuffer.resetBuffer();
        tableBuffer.setTextFormat("%25s,%15s,%15s");
        tableBuffer.addLine(String.format("%s,%s,%s","TABLE NAME", "PRELOAD COUNT", "PRELOAD ERRORS"));
        for (Table table : tables) {
            tableBuffer.addLine(String.format("%s,%d,%d", table.getName(),
                    table.getTableStats().getPreloadInserts(), table.getTableStats().getPreloadErrors()));
        }
        return tableBuffer;
    }

    public TableBuffer getRuntimeStats() {
        TableBuffer tableBuffer = outputBufferMap.getTableBuffer(OutputBufferMap.RUNTIME_STATS,
                "Runtime Table Statistics");
        tableBuffer.resetBuffer();
        tableBuffer.setTextFormat("%25s,%10s,%10s,%10s,%10s,%10s");

        tableBuffer.addLine(String.format("%s,%s,%s,%s,%s,%s", "TABLE NAME", "INSERTS", "UPDATES", "DELETES", "TOTAL", "ERRORS"));
        long inserts, updates, deletes, errors, total;
        long totalInserts = 0;
        long totalUpdates = 0;
        long totalDeletes = 0;
        long totalErrors = 0;
        long grandTotal = 0;
        for (Table table : tables) {
            inserts = table.getTableStats().getInsertCount();
            updates = table.getTableStats().getUpdateCount();
            deletes = table.getTableStats().getDeleteCount();
            total = table.getTableStats().getTotalOperations();
            errors = table.getTableStats().getErrorCount();
            totalInserts += inserts;
            totalUpdates += updates;
            totalDeletes += deletes;
            totalErrors += errors;
            grandTotal += total;
            tableBuffer.addLine(String.format("%s,%d,%d,%d,%d,%d", table.getName(),
                    inserts, updates, deletes, total, errors));
        }
        tableBuffer.addLine(String.format("%s,%d,%d,%d,%d,%d", "TOTALS",
                totalInserts, totalUpdates, totalDeletes, grandTotal, totalErrors));

        return tableBuffer;
    }

    public void logPreloadStats() {
        LOG.info(getPreloadStats().asText());
        System.out.println(getPreloadStats().asText());
    }

    public void logRuntimeStats() {
        LOG.info(getRuntimeStats().asText());
        System.out.println(getRuntimeStats().asText());
    }



}
