package qlikpe.dbloadgen.repository;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Lazy;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Lazy
@Repository
@ComponentScan(basePackages = "com.cli.jdbc.datasource")
public class LoadgenRepository {
    private final static Logger LOG = LogManager.getLogger(LoadgenRepository.class);

    private final JdbcTemplate jdbcTemplate;

    LoadgenRepository(@Qualifier("loadgenJdbcTemplate")JdbcTemplate jdbcTemplate) {
        LOG.info("creating LoadgenRepository");
        this.jdbcTemplate = jdbcTemplate;
    }


    // insert methods here
    public void placeholder() {
        if (jdbcTemplate != null) {

        }
    }

}
