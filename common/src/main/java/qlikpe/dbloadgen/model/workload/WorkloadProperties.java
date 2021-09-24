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

/**
 * This class contains configuration constants used by 
 * DbLoadgen, including property names and where appropriate
 * the default values to use in the event that a property
 * is not defined.
 */
public final class WorkloadProperties {
    /**
     * The name of the default properties "resource" to look for.
     */
    public static final String defaultProperties = "dbloadgenDefault.properties";
    /**
     * The external properties file to look for. These properties will override
     * the default properties.
     */
    public static final String externalProperties = "dbloadgen.properties";
    /**
     * The name of the file we will use to save the application context.
     */
    public static final String dbloadgenContext = "dbloadgen.xml";

    /**
     * The name of the connection that we will use.
     */
    public final static String CONNECTION_NAME = "dbloadgen.workload.connection-name";
    /**
     * A prefix that, when coupled with CONNECTION_NAME identifies the
     * properties we need in order to connect to the target.
     */
    public final static String CONNECTION_PREFIX = "dbloadgen.connection.";
    /**
     * The name of the workload configuration yaml file that defines how we will generate the load..
     */
    public final static String WORKLOAD_CONFIG_FILE = "dbloadgen.workload.config-file";


    /* *****************************************************************/

    /**
     * private to prevent explicit object creation
     */
    private WorkloadProperties() {
        super();
    }
}
