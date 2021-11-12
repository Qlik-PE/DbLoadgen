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
import qlikpe.dbloadgen.DbLoadgenProperties;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.util.*;

public class PropertyManager {
    private final static Logger LOG = LogManager.getLogger(PropertyManager.class);

    private Properties defaultProperties;
    private Properties dbloadgenProperties;

    /**
     * Read the properties specific to database processing.
     */



}
