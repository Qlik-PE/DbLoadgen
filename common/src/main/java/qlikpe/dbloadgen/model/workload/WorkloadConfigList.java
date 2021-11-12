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
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.error.YAMLException;
import qlikpe.dbloadgen.DbLoadgenProperties;

import java.io.*;
import java.util.*;

/**
 * This class reads the workload configuration yaml files from the
 * datasets directory, using them to create a list of workload
 * definitions that are available for use.
 */
public class WorkloadConfigList {
    private final static Logger LOG = LogManager.getLogger(WorkloadConfigList.class);
    private static WorkloadConfigList singleton = null;

    private Map<String, WorkloadConfig> map;

    private WorkloadConfigList() {
        reloadConfigList();
    }

    public void  reloadConfigList() {
        map = new HashMap<>();
        String[] fileNames;
        File directory;
        String defaultDatasetDir = DbLoadgenProperties.DATASET_RESOURCE_DIR;
        String datasetDirectory = DbLoadgenProperties.getInstance().getProperty(DbLoadgenProperties.DATADIR);

        String fileName, path;

        fileNames = getDirectoryFromClasspath(defaultDatasetDir, ".yml");

        for (String file : fileNames) {

            LOG.info("processing default resource config file: {}", file);
            path = String.format("%s/%s", defaultDatasetDir, file);
            InputStream is = getFileFromResourceAsStream(path);
            readWorkloadConfigYaml(file, is);
        }

        // get the files from the dataset directory that match the filter.
        FilenameFilter filter = new FilenameFilter() {
            @Override
            public boolean accept(File f, String name) {
                // We want to find only .yml files
                return name.endsWith(".yml");
            }
        };

        directory = new File(datasetDirectory);
        File[] files = directory.listFiles(filter);

        // Get the names of the files by using the .getName() method
        for (int i = 0; i < Objects.requireNonNull(files).length; i++) {
            try {
                fileName = files[i].getName();
                LOG.info("processing config file: {}", fileName);
                path = String.format("%s/%s", datasetDirectory, fileName);
                InputStream is = new FileInputStream(path);
                readWorkloadConfigYaml(fileName, is);
            } catch (Exception e) {
                LOG.error("error reading workload list: Exception: {} Message: {}", e.getCause(), e.getMessage());
            }
        }
    }

    /**
     * Read the workload config YAML file from the input stream.
     * @param fileName the file name to process.
     * @param is an InputStream to read from.
     */
    private void readWorkloadConfigYaml(String fileName, InputStream is) {
        String connectionFile = DbLoadgenProperties.getInstance().getProperty(DbLoadgenProperties.CONNECTION_LIST);
        String configName;

        if (fileName.equals(DbLoadgenProperties.CONNECTION_LIST_DEFAULT) || fileName.equals(connectionFile))
            return;

        try {
            configName = fileName.substring(0, fileName.indexOf(".yml"));
            WorkloadConfig workloadConfig;
            Yaml yaml = new Yaml(new Constructor(WorkloadConfig.class));
            workloadConfig = yaml.load(is);
            workloadConfig.setName(configName);

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
            LOG.info("adding config {} to map", configName);
            map.put(configName, workloadConfig);
        } catch (YAMLException y) {
            LOG.warn("Failed to parse yaml file: {}", y.getMessage());
        }
    }

    private InputStream getFileFromResourceAsStream(String path) {
        Resource resource = new ClassPathResource(path);
        InputStream is = null;
        try {
            is = resource.getInputStream();
        } catch (IOException e) {
            LOG.error("failed to create classpath file input stream: {}", e.getMessage());
        }
        return is;
    }

    /**
     * Locates and reads a directory (of resources) from the classpath.
     * @param directoryName the name of the directory.
     * @param endsWith an optional "endsWith" pattern that will match files
     *               we are looking for (i.e. files ending in .yml).
     * @return an array of file names.
     */
    private String[] getDirectoryFromClasspath(String directoryName, String endsWith) {
        String[] fileNames = null;
        // try spring boot resource resolver.
        ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        if (endsWith == null)
            endsWith = "*";
        String pattern = String.format("classpath*:%s/*%s", directoryName, endsWith);
        LOG.info("using Spring Boot ResourcePatternResolver: {}", pattern);
        try {
            Resource[] resources = resolver.getResources(pattern);
            if (resources.length > 0) {
                fileNames = new String[resources.length];
                for (int i = 0; i < resources.length; i++) {
                    fileNames[i] = resources[i].getFilename();
                }
            }
        } catch (IOException e) {
            LOG.warn("Did not find spring boot resource directory: {}", e.getMessage());
        }

        if (fileNames == null) {
            LOG.warn("Spring Boot ResourcePatternResolver did not return any resources for pattern {}.", pattern);
        }

        return fileNames;
    }

    /**
     * Get the Map containing the available workload configurations.
     *
     * @return a Map of WorkloadConfig instances, keyed by name.
     */
    public Map<String, WorkloadConfig> getMap() {
        return map;
    }

    /**
     * Return the names of the workload configurations as a List.
     *
     * @return a list of workload names
     */
    public List<String> getWorkloadNames() {
        return new ArrayList<>(map.keySet());
    }

    /**
     * Get an instance of WorkloadConfig from the Map.
     *
     * @param name the name of the workload
     * @return an instance of WorkloadConfig
     */
    public WorkloadConfig getWorkloadConfigByName(String name) {
        return map.get(name);
    }

    /**
     * Return a singleton instance of this class, creating the singleton if necessary.
     *
     * @return a singleton instance of WorkloadConfigList.
     */
    public static WorkloadConfigList getInstance() {
        if (singleton == null) {
            singleton = new WorkloadConfigList();
        }
        return singleton;
    }
}
