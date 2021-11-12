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
package qlikpe.dbloadgen.model.output;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class reads the workload configuration yaml files from the
 * datasets directory, using them to create a list of workload
 * definitions that are available for use.
 */
public class OutputBufferMap {
    public static final String TEST_CONNECTION = "test-connection";
    public static final String INITIALIZE_SCHEMA = "initialize-schema";
    public static final String PRELOAD_STATS = "preload-stats";
    public static final String RUNTIME_STATS = "cdc-stats";
    public static final String CLEANUP = "cleanup";


    private final static Logger LOG = LogManager.getLogger(OutputBufferMap.class);
    private static OutputBufferMap singleton = null;

    private final Map<String, OutputBuffer> map;

    private OutputBufferMap() {
        map = new HashMap<>();
    }

    /**
     * Get the Map containing the available workload configurations.
     *
     * @return a Map of WorkloadConfig instances, keyed by name.
     */
    public Map<String, OutputBuffer> getMap() {
        return map;
    }

    /**
     * Return the names of the workload configurations as a List.
     * @return a list of workload names
     */
    public List<String> getBufferNames() {
        return new ArrayList<>(map.keySet());
    }

    /**
     * Get an instance of WorkloadConfig from the Map.
     *
     * @param name the name of the workload
     * @return an instance of WorkloadConfig
     */
    public OutputBuffer getOutputBufferByName(String name) {
        return map.get(name);
    }

    public void resetAll() {
        map.forEach((k, v) -> v.resetBuffer());
    }

    public void clear() {
        map.clear();
    }

    public void addLine(String name, String line) {
        if (!map.containsKey(name)) {
            // we forgot to create a buffer with this name, so
            // create it now, defaulting to a TextBuffer.
            getTextBuffer(name, "Error: description was not set");
        }
        map.get(name).addLine(line);
    }

    /**
     * Create a new instance of TextBuffer with the specified name
     * if it does not already exist.
     * @param name the name of the buffer.
     * @param description a description of this buffer. The description is only set at
     *                    the time the buffer is created and not overridden if the
     *                    buffer already exists.
     * @return the requested TextBuffer.
     */
    public TextBuffer getTextBuffer(String name, String description) {
        TextBuffer buf;
        if (!map.containsKey(name)) {
            buf = new TextBuffer(name);
            buf.setDescription(description);
            map.put(name, buf);
        } else {
            buf = (TextBuffer)map.get(name);
        }
        return buf;
    }

    /**
     * Create a new instance of TableBuffer with the specified name
     * if it does not already exist.
     * @param name the name of the buffer
     * @param description a description of this buffer. The description is only set at
     *                    the time the buffer is created and not overridden if the
     *                    buffer already exists.
     * @return the requested TableBuffer.
     */
    public TableBuffer getTableBuffer(String name, String description) {
        TableBuffer buf;
        if (!map.containsKey(name)) {
            buf = new TableBuffer(name);
            buf.setDescription(description);
            map.put(name, buf);
        } else {
            buf = (TableBuffer)map.get(name);
        }
        return buf;
    }

    /**
     * Return the contents of the buffer formatted for output to the console.
     * @param name the name of the buffer to output.
     * @return the data formatted for output.
     */
    public String asText(String name) {
        return map.get(name).asText();
    }

    /**
     * Return the contents of the buffer formatted for output to the console.
     * @param name the name of the buffer to output.
     * @return the data formatted for output.
     */
    public String asHtml(String name) {
        return map.get(name).asHtml();
    }

    /**
     * Return a singleton instance of this class, creating the singleton if necessary.
     * @return a singleton instance of WorkloadConfigList.
     */
    public static OutputBufferMap getInstance() {
        if (singleton == null) {
            singleton = new OutputBufferMap();
        }
        return singleton;
    }
}
