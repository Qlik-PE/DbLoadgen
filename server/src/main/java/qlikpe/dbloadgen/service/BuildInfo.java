package qlikpe.dbloadgen.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.springframework.boot.autoconfigure.condition.*;
import org.springframework.boot.info.*;
import org.springframework.stereotype.*;

@Service
@ConditionalOnResource(resources="${spring.info.build.location:classpath:META-INF/build-info.properties}")
public final class BuildInfo {
    private static final Logger LOG = LogManager.getLogger(BuildInfo.class);

    private final BuildProperties buildProperties;

    public BuildInfo(BuildProperties buildProperties) {
       this.buildProperties = buildProperties;
       LOG.info("Loadgen version: {}, build time: {}", buildProperties.getVersion(), buildProperties.getTime());
    }

    public BuildProperties getBuildProperties() {
       return buildProperties;
    }
}
