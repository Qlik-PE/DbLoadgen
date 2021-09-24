package qlikpe.dbloadgen.model.database;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DatabaseConnection {
    private static final Logger LOG = LogManager.getLogger(DatabaseConnection.class);

    private final String jdbcDriver;
    private final String url;
    private final String userName;
    private final String password;
    private final String databaseType;

    private Connection connection;
    private String connectionStatus;

    public DatabaseConnection(String databaseType, Properties connectionInfo) {
        this.databaseType = databaseType;
        this.jdbcDriver = checkKey(connectionInfo, "jdbcDriver", "org.h2.Driver");
        this.url = checkKey(connectionInfo, "url", "jdbc:h2:mem:testdb");
        this.userName = checkKey(connectionInfo, "userName", "sa");
        this.password = checkKey(connectionInfo, "password", "sa");
    }

    /**
     * Get the current database connection.
     * @return the the connection.
     */
    public Connection getConnection() {
        return connection;
    }

    /**
     * Return a representation of the connection status as a string.
     * @return the connection status.
     */
    public String getConnectionStatus() {
        return connectionStatus;
    }

    /**
     * Connect to the database.
     *
     * @return an instance of Connection. Null if the attempt failed.
     */
    public Connection connect() {
        try {
            // register the JDBC driver
            Class.forName(jdbcDriver);

            // open a connection
            LOG.debug("Connecting to database: " + url);
            connection = DriverManager.getConnection(url, userName, password);
            LOG.debug("Successfully connected to database: " + url);
            connectionStatus = "connected";
        } catch (ClassNotFoundException e) {
            LOG.error("Class Not Found exception", e);
            connection = null;
            connectionStatus = "class not found: " + jdbcDriver;
        } catch (SQLException se) {
            LOG.error("SQL Exception: could not connect to DB", se);
            connection = null;
            connectionStatus = "failed";
        }
        return connection;
    }

    /**
     * Disconnect from the database.
     */
    public void disconnect() {
        try {
            if (connection != null) {
                connection.close();
                connection = null;
                connectionStatus = "disconnected";
                LOG.debug("successfully disconnected from database: " + url);
            }
        } catch(SQLException se) {
            LOG.error("SQL Exception: could not close connection", se);
            connectionStatus = "failed disconnect";
        }
    }

    /**
     * Open a test connection to validate all is well.
     * @return true if we were able to connect, false otherwise.
     */
    public boolean testConnection() {
        boolean rval;

        LOG.info("testing database connectivity for database: " + url);
        Connection conn = connect();
        if (conn != null) {
            disconnect();
            rval = true;
            LOG.info("database connection achieved for database: " + url);
        } else {
            rval = false;
            LOG.error("database connection failed for database: " + url);
        }
        return rval;
    }

    /**
     * Check if property has been initialized.
     * If the property has not been initialized, log a warning and set
     * a default value
     * @param props the properties Map
     * @param key the key to check
     * @param defaultValue the value to default to if the key is not present
     * @return the property value
     */
    private String checkKey(Properties props, String key, String defaultValue) {
        String rval;
        if (props.containsKey(key)) {
            rval = props.getProperty(key);
        } else {
            LOG.warn("{}.{} configuration property not specified. Defaulting to {}.", databaseType, key, defaultValue);
            rval = defaultValue;
        }
        return rval;
    }
}
