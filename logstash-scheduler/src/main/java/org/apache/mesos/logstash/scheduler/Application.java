package org.apache.mesos.logstash.scheduler;

import org.apache.mesos.logstash.config.LogstashSettings;
import org.apache.mesos.logstash.state.LiveState;
import org.apache.mesos.logstash.state.LogstashLiveState;
import org.apache.mesos.logstash.state.MockLiveState;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.embedded.jetty.JettyEmbeddedServletContainerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static java.util.Arrays.asList;

@SpringBootApplication
@ComponentScan(basePackages = {"org.apache.mesos.logstash"})
public class Application {

    private static String masterURL = null;

    private static boolean offline = true;
    private static int port;

    protected Application() {
    }

    public static void main(String[] args) throws IOException {

        SpringApplication app = new SpringApplication(Application.class);
        app.setShowBanner(false);

        masterURL = getParam(args, "-m", "mesos.logstash.masterUrl", null);
        offline = hasParam(args, "--offline", "mesos.logstash.offline");
        port = Integer.parseInt(getParam(args, "-p", "mesos.logstash.uiPort", "4080"));

        app.setWebEnvironment(!hasParam(args, "--no-ui", "mesos.logstash.noUI"));

        app.run(args);
    }

    @Bean
    @Qualifier("offline")
    public boolean getNoCluster() {
        return offline;
    }

    @Bean
    @Qualifier("masterURL")
    public String getMasterURL() {
        return masterURL;
    }

    @Bean
    public Path getConfigRootPath() {
        return Paths.get(".").toAbsolutePath().resolve("config/");
    }

    @Bean
    public LiveState getSchedulerStatus() {
        return (offline) ? new MockLiveState() : new LogstashLiveState();
    }

    @Bean
    public JettyEmbeddedServletContainerFactory jettyEmbeddedServletContainerFactory() {
        return new JettyEmbeddedServletContainerFactory(port);
    }

    @Bean
    public LogstashSettings getSetting() {
        // FIXME: Read these from commandline or system props.
        return new LogstashSettings(null, null);
    }

    private static String getParam(String[] args, String argName, String propName, String defaultValue) {

        List<String> argList = asList(args);

        int index = argList.indexOf(argName);

        if (index != -1 && argList.size() >= index) {
            return argList.get(index + 1);
        }

        String propValue = System.getProperty(propName);

        if (propValue != null) {
            return propValue;
        }

        if (defaultValue != null) {
            return defaultValue;
        }
        else {
            System.err.println(String
                .format("Expected: argument '%s' or system property: '%s'", argName, propName));

            System.exit(1);

            return "";
        }
    }

    private static boolean hasParam(String[] args, String argName, String propName) {

        List<String> argList = asList(args);

        int index = argList.indexOf(argName);

        if (index != -1 && argList.size() >= index) {
            return true;
        }

        String propValue = System.getProperty(propName);
        return "true".equals(propValue);
    }
}
