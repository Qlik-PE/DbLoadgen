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

import java.util.Properties;

/**
 * DbLoadgen configuration properties.
 */
public class DbLoadgenProperties {


    /**
     * The directory where the dataset(s) can be found.
     */
    public static final String DATADIR = "dbloadgen.directory";
    /**
     * The default dataset
     */
    public static final String DATADIR_DEFAULT = "../datasets";

    /**
     * The dataset connection properties to use.
     */
    public static final String CONNECTION_NAME = "dbloadgen.workload.connection-name";
    /**
     * The default connection name
     */
    public static final String CONNECTION_NAME_DEFAULT = "h2mem";

    /**
     * The dbloadgen runtime configuration yaml file we want to use
     */
    public static final String WORKLOAD_CONFIG_FILE = "dbloadgen.workload.config-file";
    /**
     * The default configuration yaml file
     */
    public static final String CONFIG_FILE_DEFAULT = "test";
    /**
     * The database user
     */
    public static final String USERNAME = "dbloadgen.username";
    /**
     * The database password
     */
    public static final String PASSWORD = "dbloadgen.password";
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


    /**
     * Get the default properties.
     */
    public static Properties getDefaultProperties() {
        Properties props = new Properties();

        props.setProperty(DATADIR, DATADIR_DEFAULT);
        props.setProperty(CONNECTION_NAME, CONNECTION_NAME_DEFAULT);
        props.setProperty(WORKLOAD_CONFIG_FILE, CONFIG_FILE_DEFAULT);
        props.setProperty(CONNECTION_LIST, CONNECTION_LIST_DEFAULT);

        return props;
    }

    /**
     * private to prevent explicit object creation
     */
    private DbLoadgenProperties() { super(); }
}
