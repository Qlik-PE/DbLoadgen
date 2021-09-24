package qlikpe.dbloadgen.model.workload;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * A class that holds information about a database connection.
 */
@Getter
@Setter
@NoArgsConstructor
public class DbLoadgenConnectionInfo {
    private String name;
    private String databaseType;
    private String jdbcDriver;
    private String url;
    private String username;
    private String password;
}
