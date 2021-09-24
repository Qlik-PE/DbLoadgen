package qlikpe.dbloadgen.model.workload;

import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;


/**
 * A list of connections that have been defined from various sources. I would have
 * liked to use a Map here, but there wasn't a straightforward way to get the Snake Yaml
 * parser to create a Map. Since getConnection() should only be called
 * once, it is not a big deal from a performance perspective.
 */
@Getter
@Setter
public class DbLoadgenConnectionList {
    private final static Logger LOG = LogManager.getLogger(DbLoadgenConnectionList.class);

    private List<DbLoadgenConnectionInfo> connections;

    /**
     * Get a connection from the connection list by name.
     *
     * @param name the name of the connection.
     * @return an instance of DbLoadgenConnectionInfo, or null if the connection name was not found.
     */
    public DbLoadgenConnectionInfo getConnection(String name) {
        for (DbLoadgenConnectionInfo connection : connections) {
            if (connection.getName().equalsIgnoreCase(name)) {
                return connection;
            }
        }
        return null;
    }

    /**
     * Add a connection to the list, first deleting an existing connection if
     * the connection name already exists in the list.
     *
     * @param connection the connection to add.
     */
    public void addConnection(DbLoadgenConnectionInfo connection) {
        DbLoadgenConnectionInfo tmp = getConnection(connection.getName());
        if (tmp != null) {
            // the connection name was already specified. Delete it so we can replace
            // it with the one specified.
            connections.remove(tmp);
        }
        connections.add(connection);
    }

    /**
     * Convert this list of connections to an instance of java.util.Properties.
     * @return the generated properties.
     */
    public Properties convertToProperties() {
        String connectionName;
        Properties properties = new Properties();

        for(DbLoadgenConnectionInfo connection : connections) {
            connectionName = connection.getName();
            addProperty(properties, connectionName, "databaseType", connection.getDatabaseType());
            addProperty(properties, connectionName, "jdbcDriver", connection.getJdbcDriver());
            addProperty(properties, connectionName, "url", connection.getUrl());
            addProperty(properties, connectionName, "userName", connection.getUsername());
            addProperty(properties, connectionName, "password", connection.getPassword());
        }
        return properties;
    }

    /**
     * Add a connection property to  an instance of java.util.Properties.
     * @param properties an instance of java.util.Properties.
     * @param connectionName the name of the connection.
     * @param propertyName the connection property to set.
     * @param value the value of the property.
     */
    private void addProperty(Properties properties, String connectionName, String propertyName, String value) {
        String prefix = "dbloadgen.connection";
        String property = String.format("%s.%s.%s", prefix, connectionName, propertyName);
        properties.put(property, value);
    }

    /**
     * Create an instance of this class from the specified yaml file.
     * @param fileName the name of the file to read / parse.
     * @return an instance of DbLoadgenConnectionList.
     */
    public static DbLoadgenConnectionList parseConnectionListYaml(String fileName) {
        DbLoadgenConnectionList connectionList;
        try {
            InputStream is = new FileInputStream(fileName);
            LOG.debug("parsing connection info file {}", fileName);
            Yaml yaml = new Yaml(new Constructor(DbLoadgenConnectionList.class));
            connectionList = yaml.load(is);
            LOG.debug("connection info file {} has been parsed: connections.size(): {}",
                    fileName, connectionList.getConnections().size());

        } catch (FileNotFoundException e) {
            LOG.warn("connection info file {}: {}", fileName, e.getMessage());
            connectionList = null;
        }
        return connectionList;
    }
}
