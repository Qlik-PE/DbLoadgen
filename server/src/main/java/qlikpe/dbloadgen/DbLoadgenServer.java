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

import qlikpe.dbloadgen.config.ini.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.*;
import io.undertow.server.*;
import io.undertow.websockets.jsr.*;
import org.springframework.boot.Banner.*;
import org.springframework.boot.autoconfigure.*;
import org.springframework.boot.builder.*;
import org.springframework.boot.context.event.*;
import org.springframework.boot.web.embedded.undertow.*;
import org.springframework.boot.web.server.*;
import org.springframework.context.*;
import org.springframework.context.annotation.*;
import org.springframework.core.*;
import org.springframework.core.env.*;
import org.springframework.web.servlet.config.annotation.*;

import java.io.*;
import java.nio.charset.*;
import java.util.Objects;
import java.util.stream.*;

@SpringBootApplication
public class DbLoadgenServer {
  private final static Logger LOG = LogManager.getLogger(DbLoadgenServer.class);

  public static void main(String[] args) {
    if (args == null)
      LOG.error("args was null");
    createApplicationBuilder()
      .run(args);
  }

  public static SpringApplicationBuilder createApplicationBuilder() {
    return new SpringApplicationBuilder(DbLoadgenServer.class)
      .bannerMode(Mode.OFF)
      .listeners(new EnvironmentSetupListener(),
              new LoggingConfigurationListener());
  }

  @Bean
  public WebServerFactoryCustomizer<UndertowServletWebServerFactory> deploymentCustomizer() {
    return factory -> {
      final UndertowDeploymentInfoCustomizer customizer = deploymentInfo -> {
        var inf = new WebSocketDeploymentInfo();
        //inf.setBuffers(new DefaultByteBufferPool(false, 64));
        inf.setBuffers(new DefaultByteBufferPool(false, 64));
        deploymentInfo.addServletContextAttribute(WebSocketDeploymentInfo.ATTRIBUTE_NAME, inf);
      };
      factory.addDeploymentInfoCustomizers(customizer);
    };
  }

  @Bean
  public WebMvcConfigurer webConfig() {
    return new WebMvcConfigurer() {
      @Override
      public void configureContentNegotiation(ContentNegotiationConfigurer configurer) {
        //configurer.favorPathExtension(false); function deprecated. setting to false no longer needed.
      }
    };
  }

  private static final class LoggingConfigurationListener implements ApplicationListener<ApplicationEnvironmentPreparedEvent>, Ordered {
    private static final String PROP_LOGGING_FILE = "logging.file";
    private static final String PROP_LOGGER = "LOGGER";
    private static final String PROP_SPRING_BOOT_LOG_LEVEL = "logging.level.org.springframework.boot";

    @Override
    public int getOrder() {
      // LoggingApplicationListener runs at HIGHEST_PRECEDENCE + 11.  This needs to run before that.
      return Ordered.HIGHEST_PRECEDENCE;
    }

    @Override
    public void onApplicationEvent(ApplicationEnvironmentPreparedEvent event) {
      final var environment = event.getEnvironment();
      final var loggingFile = environment.getProperty(PROP_LOGGING_FILE);
      if (loggingFile != null) {
        System.setProperty(PROP_LOGGER, "FILE");
        try {
          System.setProperty("logging.dir", new File(loggingFile).getParent());
        } catch (Exception ex) {
          System.err.println("Unable to set up logging.dir from logging.file " + loggingFile + ": " +
                                 Throwables.getStackTraceAsString(ex));
        }
      }
      if (environment.containsProperty("debug") &&
          !"false".equalsIgnoreCase(environment.getProperty("debug", String.class))) {
        System.setProperty(PROP_SPRING_BOOT_LOG_LEVEL, "DEBUG");
      }
    }
  }

  private static final class EnvironmentSetupListener implements ApplicationListener<ApplicationEnvironmentPreparedEvent>, Ordered {
    private static final String SM_CONFIG_DIR = "sm.config.dir";
    private static final String CONFIG_SUFFIX = "-config.ini";

    @Override
    public int getOrder() {
      return Ordered.HIGHEST_PRECEDENCE + 10;
    }

    @Override
    public void onApplicationEvent(ApplicationEnvironmentPreparedEvent event) {
      final var environment = event.getEnvironment();

      if (environment.containsProperty(SM_CONFIG_DIR)) {
        Stream.of("dbloadgen", "global")
            .map(name -> readProperties(environment, name))
            .filter(Objects::nonNull)
            .forEach(iniPropSource -> environment.getPropertySources()
                .addBefore("applicationConfigurationProperties", iniPropSource));
      }
    }

    private static IniFilePropertySource readProperties(Environment environment, String name) {
      final var file = new File(environment.getProperty(SM_CONFIG_DIR), name + CONFIG_SUFFIX);
      if (file.exists() && file.canRead()) {
        try (var in = new FileInputStream(file);
             var reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {
          return new IniFilePropertySource(name, new IniFileReader().read(reader), environment.getActiveProfiles());
        } catch (IOException ex) {
          LOG.error("Unable to read configuration file {}: {}", file, ex);
        }
      }
      return null;
    }
  }
}
