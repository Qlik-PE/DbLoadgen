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

import java.io.*;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.CountDownLatch;

@Getter
public class WorkloadManager {
    private final static Logger LOG = LogManager.getLogger(WorkloadManager.class);

    private final PropertyManager properties;
    private final Properties runtimeProperties;
    private DatabaseConnection databaseConnection;
    private Database database;
    private String connectionName;
    private String runtimeConfig;
    private final List<Table> tables;
    private WorkloadConfig workloadConfig;
    private Properties connectionInfo;
    private int batchSize;
    private int batchSleep;
    private int threadSleep;
    private int workers;
    private String databaseType;
    private DataTypeMapper dataTypeMapper;

    /**
     * Constructor that does not initialize runtime properties
     * at construction time.
     */
    public WorkloadManager() {
        properties = new PropertyManager(null);
        tables = new ArrayList<>();
        runtimeProperties = null;
    }

    /**
     * Constructor that initializes runtime properties.
     *
     * @param runtimeProperties runtime properties that will override defaults.
     */
    public WorkloadManager(Properties runtimeProperties) {
        this.runtimeProperties = runtimeProperties;
        properties = new PropertyManager(runtimeProperties);
        configureProperties(runtimeProperties);
        tables = new ArrayList<>();
    }

    /**
     * Configures properties that come along at runtime, either from the GUI
     * from the command line, or from a REST call.
     * Calls to this function are additive ... new properties
     * will be added, existing properties will be replaced.
     */
    public void configureProperties(Properties props) {
        Properties outputProps = properties.getLoadgenProperties();
        overrideProperty(props, DbLoadgenProperties.CONNECTION_NAME, outputProps, WorkloadProperties.CONNECTION_NAME);
        overrideProperty(props, DbLoadgenProperties.WORKLOAD_CONFIG_FILE, outputProps, WorkloadProperties.WORKLOAD_CONFIG_FILE);
    }

    /**
     * Initialize the workload.
     * @throws WorkloadInitializationException on a configuration error.
     */
    public void initWorkload() throws WorkloadInitializationException {
        getConnectionInfo();
        getWorkloadConfig();
    }

    /**
     * Ensures we can connect to the database.
     */
    public void testConnection() {
        databaseConnection.testConnection();
    }

    /**
     * Initialize the target database schema.
     */
    public void initSchema() {
        Connection connection = databaseConnection.connect();
        if (connection != null) {
            String schemaName = workloadConfig.getSchema();
            database.createSchema(connection, schemaName);

            for (Table table : tables) {
                database.createTable(connection, table);
                database.addRandomizer(connection, table);
            }
        } else {
            LOG.error("could not connect to the target database");
        }
    }

    /**
     * Cleanup any remnants from a prior run.
     */
    public void cleanup() {
        Connection connection = databaseConnection.connect();
        if (connection != null) {
            String schemaName = workloadConfig.getSchema();

            for (Table table : tables) {
                database.dropTable(connection, table);
            }
            database.dropSchema(connection, schemaName);
        } else {
            LOG.error("could not connect to the target database");
        }
    }

    /**
     * Preload the target schema prior to running the test.
     * This method first checks to be sure that the schema has
     * been initialized.
     */
    public void preloadTables() {
        int preloadWorkers = workloadConfig.getPreloadThreads();
        int numTables = tables.size();
        int activeThreads = 0;
        int latchCount;
        String tableName;

        CountDownLatch latch = new CountDownLatch(numTables);
        for (int i = 0; i < numTables; ) {
            tableName = tables.get(i).getName();
            latchCount = (int) latch.getCount();
            // start and manage preload worker threads.
            if (activeThreads < preloadWorkers) {
                // schedule a thread for this table
                String threadName = String.format("preload-%s", tableName);
                Thread t = new Thread(new PreloadThread(threadName, latch, workloadConfig,
                        databaseType, connectionInfo, tables.get(i)));
                t.start();
                activeThreads++;
                i++;
            } else {
                // wait for a thread to end before scheduling another thread
                int newCount = (int) latch.getCount();
                LOG.debug("before while: table: {} latchCount: {} newCount: {}", tableName, latchCount, newCount);
                while (latchCount == newCount) {
                    LOG.debug("table: {} latchCount: {} newCount: {}", tableName, latchCount, newCount);
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        LOG.warn("interrupt received during preload of table " + tableName, e);
                    }
                    newCount = (int) latch.getCount();
                }
                activeThreads = activeThreads - (latchCount - newCount);
            }
        }
        // now wait for all threads to exit.
        try {
            LOG.debug("waiting for preload countdown latch. Count {}", latch.getCount());
            latch.await();
        } catch (InterruptedException ex) {
            LOG.warn("interrupt received during preload latch wait", ex);
        }
    }

    /**
     * Run the test. It will first initialize the database
     * if the database has not already been set up. This allows
     * this function to be called whether or not the initialization
     * workflow has been performed.
     */
    public void runTest() {
        int currentWorker;
        long duration = (long) workloadConfig.getDuration() * 60 * 1000;
        CountDownLatch latch = new CountDownLatch(workers);
        LOG.info("workers will run for {} minutes", workloadConfig.getDuration());

        List<List<Table>> workerTables = distributeTables();

        for (currentWorker = 0; currentWorker < workers; currentWorker++) {
            String threadName = String.format("worker-%d", currentWorker);
            Thread t = new Thread(new WorkerThread(threadName, latch,
                    workloadConfig, databaseType, connectionInfo, workerTables.get(currentWorker)));
            Timer timer = new Timer();
            timer.schedule(new WorkerTimeoutTask(t, timer), duration);
            t.start();
        }
        // now wait for all threads to exit.
        try {
            latch.await();
            LOG.debug("latch.await() triggered");
        } catch (InterruptedException ex) {
            LOG.warn("interrupt received during runtime latch wait", ex);
        }
        LOG.debug("exiting WorkloadManager.run()");
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
        Properties props = properties.getPropertySubset(WorkloadProperties.CONNECTION_PREFIX + connectionName,
                true);
        overrideProperty(runtimeProperties, DbLoadgenProperties.DATABASE_TYPE, props, "databaseType");
        overrideProperty(runtimeProperties, DbLoadgenProperties.JDBC_DRIVER, props, "jdbcDriver");
        overrideProperty(runtimeProperties, DbLoadgenProperties.URL, props, "url");
        overrideProperty(runtimeProperties, DbLoadgenProperties.USERNAME, props, "username");
        overrideProperty(runtimeProperties, DbLoadgenProperties.PASSWORD, props, "password");

        properties.printProperties(props, "Connection Info");
        return props;
    }

    /**
     * Get the connection info from the properties file.
     *
     * @throws WorkloadInitializationException if there are problems with the connection info.
     */
    private void getConnectionInfo() throws WorkloadInitializationException {
        connectionName = properties.getProperty(WorkloadProperties.CONNECTION_NAME);
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
     * Get the dataset configuration.
     *
     * @throws WorkloadInitializationException if the dataset cannot be configured.
     */
    private void getWorkloadConfig() throws WorkloadInitializationException {
        String fileName;
        runtimeConfig = properties.getProperty(WorkloadProperties.WORKLOAD_CONFIG_FILE);
        if (runtimeConfig == null) {
            String message = "configuration file was not specified";
            LOG.error(message);
            throw new WorkloadInitializationException(message);
        }

        // process the workload configuration file
        fileName = String.format("datasets/%s.yml", runtimeConfig);
        try {
            Yaml yaml = new Yaml(new Constructor(WorkloadConfig.class));
            InputStream is = getFileAsInputStream(fileName);
            workloadConfig = yaml.load(is);
            batchSize = workloadConfig.getWorkerBatchSize();
            batchSleep = workloadConfig.getWorkerBatchSleep();
            threadSleep = workloadConfig.getWorkerThreadSleep();
            workers = workloadConfig.getWorkerThreads();

            if (workloadConfig.getOperationPct() == null) {
                LOG.info("workload configuration operation pct not set using defaults I(25) U(50) D(25)");
                workloadConfig.setOperationPct(new OperationPct(25, 50, 25));
            } else {
                LOG.info("Operation PCT: insert({}) update({}) delete ({})",
                        workloadConfig.getOperationPct().getInsert(),
                        workloadConfig.getOperationPct().getUpdate(),
                        workloadConfig.getOperationPct().getDelete());
            }

            if (workloadConfig.getAllTablesAllThreads() == null) {
                LOG.info("workload configuration allTablesAllThreads not set; using default: false");
                workloadConfig.setAllTablesAllThreads("false");
            }

        } catch (IOException e) {
            String message = "could not open dataset resource configuration: " + fileName;
            LOG.error(message, e);
            throw new WorkloadInitializationException(message);
        }

        // now ingest the configurations for the specified tables.
        String schemaName = workloadConfig.getSchema();
        for (TableConfig tableConfig : workloadConfig.getTables()) {
            fileName = String.format("datasets/%s/%s.yml", runtimeConfig, tableConfig.getName());
            try {
                Yaml yaml = new Yaml(new Constructor(Table.class));
                InputStream is = getFileAsInputStream(fileName);
                Table table = yaml.load(is);
                table.setTableConfig(tableConfig);
                table.setSchemaName(schemaName);
                table.setDatabase(database);
                table.getTableStats().initMetaData(schemaName, table.getName());
                table.setColumnTypes(dataTypeMapper);
                tables.add(table);
            } catch (IOException e) {
                String message = "could not open dataset table configuration: " + fileName;
                LOG.error(message, e);
                throw new WorkloadInitializationException(message);
            }
        }
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
        } else {
            return inputStream;
        }

    }

    /**
     * Get a file from the resources folder.
     *
     * @param fileName the name of the file.
     * @return an InputStream;
     */
    private InputStream getFileAsInputStream(String fileName) throws FileNotFoundException {
        return new FileInputStream(fileName);
    }

    public void logPreloadStats() {
        String dash = "-";
        String header;
        String divider;
        String stats;

        header = String.format("%25s %15s %15s", "TABLE", "PRELOAD COUNT", "PRELOAD ERRORS");
        divider = dash.repeat(header.length());
        LOG.info(divider);
        LOG.info(header);
        LOG.info(divider);
        for (Table table : tables) {
            stats = String.format("%25s %15d %15d", table.getName(),
                    table.getTableStats().getPreloadInserts(), table.getTableStats().getPreloadErrors());
            LOG.info(stats);
        }
        LOG.info(divider);
        LOG.info("");
    }


    public void logRunStats() {
        String dash = "-";
        String header;
        String divider;
        String stats;

        header = String.format("%25s %10s %10s %10s %10s %10s", "TABLE", "INSERTS", "UPDATES", "DELETES", "TOTAL", "ERRORS");
        divider = dash.repeat(header.length());
        LOG.info(divider);
        LOG.info(header);
        LOG.info(divider);
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
            stats = String.format("%25s %10d %10d %10d %10d %10d", table.getName(),
                    inserts, updates, deletes, total, errors);
            LOG.info(stats);
        }
        LOG.info(divider);
        stats = String.format("%25s %10d %10d %10d %10d %10d", "TOTALS",
                totalInserts, totalUpdates, totalDeletes, grandTotal, totalErrors);
        LOG.info(stats);
        LOG.info(divider);
        LOG.info("");
    }
}
