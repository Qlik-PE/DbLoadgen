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

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.reflections.scanners.SubTypesScanner;
import qlikpe.dbloadgen.model.initializer.Initializer;
import qlikpe.dbloadgen.model.workload.WorkloadInitializationException;
import qlikpe.dbloadgen.model.workload.WorkloadManager;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import static java.lang.System.exit;
import static net.sourceforge.argparse4j.impl.Arguments.store;


public class DbLoadgenCLI {
    private final static Logger LOG = LogManager.getLogger(DbLoadgenCLI.class);
    private final static Map<String, String> commandMap = Map.ofEntries(
            Map.entry("list-workloads", "list workload options that are available to the app."),
            Map.entry("list-connections", "list database connections that have been defined"),
            Map.entry("init", "create the schema and tables for this workload"),
            Map.entry("preload", "preload the tables for this workload"),
            Map.entry("reset", "perform a `cleanup', followed by an 'init' and 'preload'"),
            Map.entry("run", "run the CDC load against the database"),
            Map.entry("cleanup", "remove the workload's tables and associated schema from the database"),
            Map.entry("test-connection", "test database connectivity"),
            Map.entry("end-to-end", "run an end-to-end test: cleanup, init, preload, and run"),
            Map.entry("help-initializers", "print a brief description of the available column initializers"),
            Map.entry("help-commands", "print this brief description of COMMAND options")
            );

    /**
     * Generates an instance of ArgumentParser for parsing the
     * command line for DbLoadgen.
     * @param commandOptions a List of supported commands.
     * @param supportedDatabases a List of supported database types.
     * @return an instance of ArgumentParser.
     */
    private ArgumentParser argParser(List<String> commandOptions, List<String> supportedDatabases) {
        ArgumentParser parser = ArgumentParsers
                .newFor("DbLoadgenCLI")
                .build()
                .defaultHelp(true)
                .description("This utility generates user-configurable loads against a database.");

        parser.addArgument("command")
                .action(store())
                .choices(commandOptions)
                .required(true)
                .type(String.class)
                .metavar("COMMAND")
                .help("The runtime command to execute: " + commandOptions.toString());

        parser.addArgument("--dataset-dir")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("DATASET_DIRECTORY")
                .dest(DbLoadgenProperties.DATADIR)
                .help("The directory where the source dataset(s) are stored");

        parser.addArgument("--workload-config")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("DATASET")
                .dest(DbLoadgenProperties.WORKLOAD_CONFIG_FILE)
                .help("The name of the runtime workload configuration yaml file to use. This should be located in the DATA_DIRECTORY.");

        parser.addArgument("--connection-list")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("CONNECTION_LIST")
                .dest(DbLoadgenProperties.CONNECTION_LIST)
                .help("The name of yaml file that contains a list of database connections that have been defined. This should be located in the DATA_DIRECTORY.");

        parser.addArgument("--connection-name")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("CONNECTION_NAME")
                .dest(DbLoadgenProperties.CONNECTION_NAME)
                .help("The name of the connection from the connection yaml to use.");

        parser.addArgument("--user")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("USER_NAME")
                .dest(DbLoadgenProperties.USERNAME)
                .help("The database user name.");

        parser.addArgument("--password")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("PASSWORD")
                .dest(DbLoadgenProperties.PASSWORD)
                .help("The password for the database user");

        parser.addArgument("--url")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("JDBC_URL")
                .dest(DbLoadgenProperties.URL)
                .help("The database JDBC connection URL.");

        parser.addArgument("--jdbc-driver")
                .action(store())
                .type(String.class)
                .metavar("JDBC_DRIVER")
                .dest(DbLoadgenProperties.JDBC_DRIVER)
                .help("The fully qualified database driver class");

        parser.addArgument("--database-type")
                .action(store())
                .choices(supportedDatabases)
                .required(false)
                .type(String.class)
                .metavar("DATABASE_TYPE")
                .dest(DbLoadgenProperties.DATABASE_TYPE)
                .help("The type of database: " + supportedDatabases.toString());

        parser.addArgument("--properties-file")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("PROPERTIES_FILE")
                .dest(DbLoadgenProperties.PROPERTIES_FILE)
                .help("The path to a properties file where these options may also be set");

        return parser;
    }


    private void getInitializerReference() {
        String separator = String.format("%n");
        Reflections reflections = new Reflections(
                "qlikpe.dbloadgen.model.initializer", Scanners.SubTypes);
        Set<Class<? extends Initializer>> initializers = reflections.getSubTypesOf(Initializer.class);
        StringBuilder buffer = new StringBuilder(4096);
        for(Class<? extends Initializer> clazz: initializers) {
            try {
                //System.out.println(clazz.getName());
                Initializer type = clazz.getDeclaredConstructor().newInstance();
                buffer = type.getHelp(buffer);
                buffer.append(separator);
            } catch (InstantiationException | InvocationTargetException
                    | NoSuchMethodException | IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        System.out.println(buffer);
    }

    private void getCommandReference() {
        System.out.println();
        System.out.println("COMMAND reference:");
        System.out.println();
        for(Map.Entry<String, String> command : commandMap.entrySet()) {
            System.out.printf("   %-20s - %s%n", command.getKey(), command.getValue());
        }
        System.out.println();
    }

    public static void main(String[] args) {
        DbLoadgenCLI dbloadgen = new DbLoadgenCLI();
        List<String> commands = new ArrayList<>(commandMap.keySet());
        List<String> supportedDatabases = List.of("h2", "mysql", "postgres", "oracle", "sqlserver");

        ArgumentParser parser = dbloadgen.argParser(commands, supportedDatabases);
        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            dbloadgen.getCommandReference();
            //parser.printHelp();
            exit(1);
        }
        assert res != null;
        LOG.debug("Parser Namespace: {}", res);
        String command = res.getString("command");
        Map<String, Object> commandLineProps = res.getAttrs();

        Properties runtimeProperties = null;
        try {
            runtimeProperties = DbLoadgenProperties.getInstance().initProperties(commandLineProps);
            DbLoadgenProperties.getInstance().printProperties("final", runtimeProperties);
        } catch (IOException e) {
            System.out.println("ERROR: " + e.getMessage());
            parser.printUsage();
            dbloadgen.getCommandReference();
            exit(1);
        }

        // process any help-related commands and exit.
        switch(command) {
            case "help-initializers":
                dbloadgen.getInitializerReference();
                exit(0);
                break;
            case "help-commands":
                dbloadgen.getCommandReference();
                exit(0);
                break;
            default:
                // not a help command, so fall through.
                break;
        }
            //LOG.info("Starting standalone execution");
        WorkloadManager workload = new WorkloadManager(runtimeProperties);
        try {
            switch(command) {
                case "test-connection":
                case "cleanup":
                case "init":
                case "preload":
                case "run":
                case "reset":
                case "end-to-end":
                case "list-workloads":
                case "list-connections":
                    workload.executeCommand(command, false);
                    break;
                default:
                    LOG.error("Unrecognized command: " + command);
                    parser.printHelp();
                    break;
            }
        } catch (WorkloadInitializationException e) {
            LOG.error("failed to initialize workload", e);
        }
        LOG.debug("Exiting");
        exit(0);
    }
}
