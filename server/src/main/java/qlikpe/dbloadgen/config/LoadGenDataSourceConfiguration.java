package qlikpe.dbloadgen.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.jdbc.core.JdbcTemplate;
import qlikpe.dbloadgen.model.database.Database;

import javax.sql.DataSource;

//https://blog.virtual7.de/dynamically-change-data-source-connection-details-at-runtime-in-spring-boot/

/**
 * Change the Spring datasource dynamically at runtime so the user can
 * provide the necessary information to connect to any database.
 */
@Configuration
public class LoadGenDataSourceConfiguration {
    private final static Logger LOG = LogManager.getLogger(LoadGenDataSourceConfiguration.class);

    @Lazy
    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public DataSource loadgenDataSource(Database database) {
        LOG.info("creating custom loadgen data source");
        DataSourceBuilder<?> dsBuilder = DataSourceBuilder.create();

        // Get the database settings
        //dsBuilder.driverClassName(database.getJdbcDriver());
        //dsBuilder.url(database.getUrl());
        //dsBuilder.username(database.getUser());
        //dsBuilder.password(database.getPassword());
        return dsBuilder.build();
    }

    @Lazy
    @Qualifier("loadgenJdbcTemplate")
    @Bean
    public JdbcTemplate loadgenJdbcTemplate() {
        LOG.info("creating loadgen jdbc template");
        //return new JdbcTemplate(loadgenDataSource());
        return new JdbcTemplate();

    }

}
