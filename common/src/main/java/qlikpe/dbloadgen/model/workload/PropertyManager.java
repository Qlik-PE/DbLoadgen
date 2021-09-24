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


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.util.*;

public class PropertyManager {
    private final static Logger LOG = LogManager.getLogger(PropertyManager.class);

    private final String savedContextPath;
    private Properties defaultProperties;
    private Properties dbloadgenProperties;

    /**
     * Read the properties specific to database processing.
     */
    public PropertyManager(Properties runtimeProperties) {
        String rootPath = FileSystems.getDefault()
                .getPath("")
                .toAbsolutePath()
                .toString() + "/";
        String propertiesPath = WorkloadProperties.defaultProperties;  // properties

        savedContextPath = rootPath + WorkloadProperties.dbloadgenContext;  // saved application context
        try {
            defaultProperties = new Properties();
            InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(propertiesPath);
            defaultProperties.load(inputStream);
        } catch (IOException e) {
            LOG.error("could not access dbloadgen default properties file: " + propertiesPath, e);
        }
        try {
            dbloadgenProperties = new Properties(defaultProperties);
            if (runtimeProperties != null)
                dbloadgenProperties.putAll(runtimeProperties);
            dbloadgenProperties.loadFromXML(new FileInputStream(savedContextPath));
        } catch (IOException e) {
            LOG.warn("could not access dbloadgen saved context: " + savedContextPath);
        }
        printProperties(dbloadgenProperties, "Loadgen properties");
    }

    /**
     * Get the configured Properites.
     * @return the dbloadgenProperties.
     */
    public Properties getLoadgenProperties() {
        return dbloadgenProperties;
    }

    /**
     * Get a property from the application properties.
     * @param property the property name
     * @return the property's value.
     */
    public String getProperty(String property) {
        String value = dbloadgenProperties.getProperty(property);
        if (value == null) {
            LOG.error("property {} was not found", property);
        }
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
        Set<String> keys = dbloadgenProperties.stringPropertyNames();
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
                subset.setProperty(newKey, dbloadgenProperties.getProperty(key));
            }
        }
        if (subset.isEmpty()) {
            LOG.error("property prefix {} was not found", prefix);
        }
        return subset;
    }

    /**
     * Set a property for the application, that is either new or overrides a default.
     * @param property the property name
     * @param value the property's value
     */
    public void setProperty(String property, String value) {
        dbloadgenProperties.setProperty(property, value);
    }

    /**
     * Add the specified properties to the application properties.
     * New properties will be added if they don't exist. Otherwise existing
     * properties will be updated.
     * otherwise.
     * @param props the properties to add/update
     */
    public void setProperties(Properties props) {
        dbloadgenProperties.putAll(props);
    }

    /**
     * Save the application context so we can read/use it again later.
     * In theory this should only contain non-default properties, but need
     * to confirm.
     */
    public void saveContext() {
        try {
            dbloadgenProperties.storeToXML(new FileOutputStream(savedContextPath),
                    "saved dbloadgen application context");
        } catch (IOException e) {
            LOG.error("failed to save dbloadgen application context: " + savedContextPath, e);
        }
    }

    /**
     * Print the properties found in the specified
     * instance of Properties in sorted order.
     *
     * @param props the properties to print
     */
    public void printProperties(Properties props, String msg) {

        SortedSet<String> keySet = new TreeSet<>(props.stringPropertyNames());

        LOG.info("***** Dumping properties: {} ******", msg);
        LOG.info("************************");
        LOG.info("*** Begin Properties ***");
        LOG.info("************************");

        for (String s : keySet) {
            LOG.info("*** {} = {}", s, props.getProperty(s));
        }
        LOG.info("*************************");
        LOG.info("*** End of Properties ***");
        LOG.info("*************************");
    }
}
