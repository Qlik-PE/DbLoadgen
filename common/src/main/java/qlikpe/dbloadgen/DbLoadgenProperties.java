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

package qlikpe.dbloadgen;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import qlikpe.dbloadgen.model.workload.DbLoadgenConnectionList;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import static java.lang.Integer.max;

/**
 * DbLoadgen configuration properties.
 */
public class DbLoadgenProperties {
    private final static Logger LOG = LogManager.getLogger(DbLoadgenProperties.class);
    private static DbLoadgenProperties singleton = null;
    private Properties dbloadgenProps;
    private DbLoadgenConnectionList connectionList;
    private final String savedContextPath;


    /**
     * The directory where the dataset(s) can be found.
     */
    public static final String DATADIR = "dbloadgen.directory";
    /**
     * The default dataset
     */
    public static final String DATADIR_DEFAULT = "datasets";

    /**
     * Where to look for datasets in the classpath.
     */
    public static final String DATASET_RESOURCE_DIR = "datasets";

    /**
     * The name of the connection that we will use.
     */
    public static final String CONNECTION_NAME = "dbloadgen.workload.connection-name";
    /**
     * The default connection name
     */
    public static final String CONNECTION_NAME_DEFAULT = "h2mem";
    /**
     * A prefix that, when coupled with CONNECTION_NAME identifies the
     * properties we need in order to connect to the target.
     */
    public final static String CONNECTION_PREFIX = "dbloadgen.connection.";
    /**
     * The JDBC driver class.
     */
    public static final String JDBC_DRIVER = "dbloadgen.connection.jdbcDriver";
    /**
     * The connection URL
     */
    public static final String URL = "dbloadgen.connection.url";
    /**
     * The database type
     */
    public static final String  DATABASE_TYPE = "dbloadgen.connection.databaseType";
    /**
     * The database user
     */
    public static final String USERNAME = "dbloadgen.connection.userName";
    /**
     * The database password
     */
    public static final String PASSWORD = "dbloadgen.connection.password";


    /**
     * The name of the workload configuration yaml file that defines how we will generate the load..
     */
    public static final String WORKLOAD_CONFIG_FILE = "dbloadgen.workload.config-file";
    /**
     * The default configuration yaml file
     */
    public static final String CONFIG_FILE_DEFAULT = "test";


    /**
     * The path to an external properties file where the default values may
     * be overridden.
     */
    public static final String  PROPERTIES_FILE = "dbloadgen.propertiesFile";

    /**
     * The name of an optional yaml file that contains connection information.
     */
    public static final String CONNECTION_LIST = "dbloadgen.connectionList";

    /**
     * The default name of the connection list yaml file if not overridden.
     */
    public static final String CONNECTION_LIST_DEFAULT = "connections.yml";

    /************************************************/
    public static final String DEFAULT_PROPERTIES = "dbloadgenDefault.properties";

    /**
     * The name of the file we will use to save the application context.
     */
    public static final String dbloadgenContext = "dbloadgen.xml";



    /**
     * Return the singleton that handles the properties.
     * @return a singleton instance of DbloadgenProperties.
     */
    public static DbLoadgenProperties getInstance() {
        if (singleton == null)
            singleton = new DbLoadgenProperties();

        return singleton;
    }

    /**
     * Get the properties that have been set for DbLoadgen.
     * @return an instance of Properties.
     */
    public Properties getProperties() {
        return dbloadgenProps;
    }

    /**
     * Get the value of a property.
     * @param key the property's key
     * @return the property value.
     */
    public String getProperty(String key) {
        return dbloadgenProps.getProperty(key);
    }

    /**
     * Get the list of connections defined in the connections.yml file.
     * @return the connection list.
     */
    public DbLoadgenConnectionList getConnectionList() { return connectionList; }

    /**
     * private to prevent explicit object creation
     */
    private DbLoadgenProperties() {
        super();
        String rootPath = FileSystems.getDefault()
                .getPath("")
                .toAbsolutePath()
                .toString() + "/";

        savedContextPath = rootPath + DbLoadgenProperties.dbloadgenContext;  // saved application context
    }



    /**
     * Get the default properties.
     */
    private Properties getDefaultPropertyValues() {
        Properties props = readPropertiesFromClassPath(DbLoadgenProperties.DEFAULT_PROPERTIES);

        props.setProperty(DATADIR, DATADIR_DEFAULT);
        props.setProperty(CONNECTION_NAME, CONNECTION_NAME_DEFAULT);
        props.setProperty(WORKLOAD_CONFIG_FILE, CONFIG_FILE_DEFAULT);
        props.setProperty(CONNECTION_LIST, CONNECTION_LIST_DEFAULT);

        return props;
    }


    /**
     * Assembles the application properties from various sources:
     * <ul>
     *     <li>default properties</li>
     *     <li>external properties file</li>
     *     <li>command line arguments</li>
     * </ul>
     * @param commandLineProps properties parsed from the command line.
     * @return an instance of java.util.Properties
     * @throws IOException for invalid command line arguments.
     */
    public Properties initProperties(Map<String, Object> commandLineProps) throws IOException {

        String propertiesFile = (String)commandLineProps.get(DbLoadgenProperties.PROPERTIES_FILE);

        // initialize with the default properties
        dbloadgenProps = new Properties(getDefaultPropertyValues());

        // now override the defaults with anything specified in an external properties file
        if (propertiesFile != null) {

            FileInputStream fis = new FileInputStream(propertiesFile);
            Properties props = new Properties();
            props.load(fis);
            for (Map.Entry<Object, Object> entry : props.entrySet()) {
                if (entry.getValue() != null) {
                    System.out.println("Key:" + entry.getKey());
                    System.out.println("Value:" + entry.getValue());
                    dbloadgenProps.setProperty((String) entry.getKey(), (String) entry.getValue());
                }
            }
            //printProperties("properties file", props);
        }

        // get any connections that might be specified in a
        // connections.yml file in the dataset directory.
        String inputDirectory = getIntermediateProperty(commandLineProps, dbloadgenProps, DbLoadgenProperties.DATADIR);
        String fileName = getIntermediateProperty(commandLineProps, dbloadgenProps, DbLoadgenProperties.CONNECTION_LIST);
        String path = String.format("%s/%s", inputDirectory, fileName);
        connectionList = DbLoadgenConnectionList.parseConnectionListYaml(path);

        if (connectionList != null) {
            LOG.debug("Connection list size: {}", connectionList.getConnections().size());
            // there was a file to parse
            Properties properties = connectionList.convertToProperties();
            if (properties.size() > 0) {
                dbloadgenProps.putAll(properties);
                //printProperties("connection-list-file", properties);
            } else {
                LOG.warn("connections list property size was 0");
            }
        } else {
            LOG.warn("connections list was null!!!");
        }

        // now add any saved context from prior runs.
        dbloadgenProps.putAll(readContext());

        // finally, override properties with anything specified on the command line
        for (Map.Entry<String, Object> entry : commandLineProps.entrySet()) {
            if (entry.getValue() != null) {
                dbloadgenProps.setProperty(entry.getKey(), (String)entry.getValue());
            }
        }

        // update the connections in the connection list with any properties that might override those values.
        connectionList.convertFromProperties(this);

        return dbloadgenProps;
    }

    /**
     * Add the specified properties to the application properties.
     * New properties will be added if they don't exist. Otherwise existing
     * properties will be updated.
     * otherwise.
     * @param props the properties to add/update
     */
    public void setProperties(Properties props) {
        dbloadgenProps.putAll(props);
    }

    /**
     * Set the requested property to the given value.
     * @param property the property to set.
     * @param value the value of the property.
     */
    public void setProperty(String property, String value) {
        dbloadgenProps.setProperty(property, value);
    }

    /**
     * Get an intermediate value from either the command line or from the current properties if not specified.
     * @param commandLineProps properties entered on the command line
     * @param dbLoadgenProps the currently defined properties.
     * @param key the key to look for.
     * @return the value of the property.
     */
    private String getIntermediateProperty(Map<String, Object> commandLineProps, Properties dbLoadgenProps, String key) {
        String value;
        if (commandLineProps.containsKey(key) && (commandLineProps.get(key) != null))
            value = (String)commandLineProps.get(key);
        else value = dbLoadgenProps.getProperty(key);
        return value;
    }

    /**
     * Get a subset of properties that begin with prefix, stripping off the prefix
     * in the process.
     * @param prefix the prefix we are looking for
     * @param trimPrefix true: trim the prefix from the returned properties
     * @return the subset of properties as a Map.
     */
    public Properties getPropertySubset(String prefix, boolean trimPrefix) {
        Properties subset = new Properties();
        Set<String> keys = dbloadgenProps.stringPropertyNames();
        String newKey;

        if (prefix.charAt(prefix.length() - 1) != '.') {
            // prefix does not end in a dot, so add one.
            prefix = prefix + '.';
        }

        for (String key: keys) {
            if (key.startsWith(prefix)) {

                if (trimPrefix)
                    newKey = key.substring(prefix.length());
                else newKey = key;
                subset.setProperty(newKey, dbloadgenProps.getProperty(key));
            }
        }
        if (subset.isEmpty()) {
            LOG.error("property prefix {} was not found", prefix);
        }
        return subset;
    }

    public Properties readContext() {
        Properties properties = new Properties();
        try {
            LOG.info("Reading dbloadgen saved context");
            properties.loadFromXML(new FileInputStream(savedContextPath));
        } catch (IOException e) {
            LOG.warn("could not access dbloadgen saved context: " + savedContextPath);
        }
        return properties;
    }

    /**
     * Save the application context so we can read/use it again later.
     * In theory this should only contain non-default properties, but need
     * to confirm.
     */
    public void saveContext() {
        try {
            dbloadgenProps.storeToXML(new FileOutputStream(savedContextPath),
                    "saved dbloadgen application context");
        } catch (IOException e) {
            LOG.error("failed to save dbloadgen application context: " + savedContextPath, e);
        }
    }

    private Properties readPropertiesFromClassPath(String file) {
        Properties properties = new Properties();
        try {
            InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(file);
            properties.load(inputStream);
        } catch (IOException e) {
            LOG.error("could not access dbloadgen default properties file: " + file, e);
        }
        return properties;
    }

    /**
     * Print the properties in this instance of java.util.properties.
     * @param msg a message to surround this dump of the properties.
     * @param props the instance of java.util.properties to dump.
     */
    public void printProperties(String msg, Properties props) {
        final String NEWLINE = System.lineSeparator();

        int maxWidth = 0;

        TreeMap<Object, Object> sortedProperties = new TreeMap<>(props);
        for(Map.Entry<Object, Object> entry : sortedProperties.entrySet()) {
            maxWidth = max(maxWidth, entry.getKey().toString().length());
        }
        String header = String.format("Dumping properties: %s ***", msg);
        String footer = String.format("End of dump: %s        ***", msg);
        String divider = "*".repeat(max(maxWidth, header.length()));
        StringBuilder builder = new StringBuilder();
        builder.append(NEWLINE);
        builder.append(divider).append(NEWLINE);
        builder.append(header).append(NEWLINE);
        builder.append(divider).append(NEWLINE);

        String formatString = String.format("%s%ds => %s", "%-", maxWidth, "%s");
        for(Map.Entry<Object, Object> entry : sortedProperties.entrySet()) {
            builder.append(String.format(formatString, entry.getKey(), entry.getValue())).append(NEWLINE);
        }
        builder.append(divider).append(NEWLINE);
        builder.append(footer).append(NEWLINE);
        builder.append(divider).append(NEWLINE);

        LOG.info(builder.toString());
        System.out.println(builder);
    }

}
