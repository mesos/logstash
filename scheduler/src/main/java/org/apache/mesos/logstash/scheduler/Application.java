package org.apache.mesos.logstash.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Scope;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SpringBootApplication
@EnableAutoConfiguration
@ComponentScan
public class Application {

    private static final Logger LOGGER = LoggerFactory.getLogger(Scheduler.class);

    private static String masterURL = null;

    protected Application() {}

    public static void main(String[] args) throws IOException {

        readConfigFromCommandLine(args);

        SpringApplication app = new SpringApplication(Application.class);
        app.setShowBanner(false);
        app.run(args);
    }

    @Bean
    @Scope("prototype")
    public ExecutorService getExecutor() {
        return Executors.newSingleThreadExecutor();
    }

    @Bean
    @Qualifier("masterURL")
    public String getMasterURL() {
        return masterURL;
    }

    @Bean
    @Qualifier("host")
    public ConfigMonitor createHostMonitor() {
        return new ConfigMonitor("config/host");
    }

    @Bean
    @Qualifier("docker")
    public ConfigMonitor createDockerMonitor() {
        return new ConfigMonitor("config/docker");
    }

    @Bean
    public Driver getDriver() {
        // FIXME: Hack for testing frontend.
        if (true) {
            return new NoopDriver();
        }
        else {
            return new MesosDriver(masterURL);
        }
    }

    private static void readConfigFromCommandLine(String... args) {
        List<String> argList = Arrays.asList(args);
        int index = argList.indexOf("-m");

        if (index != -1 && argList.size() >= index) {
            masterURL = argList.get(index + 1);
            LOGGER.debug("MasterURL configured as: '%s'", masterURL);
        }
        else {
            printUsage();
        }
    }

    private static void printUsage() {
        System.err.println("Usage: logstash-scheduler -m <master-url>");
        System.exit(1);
    }
}
