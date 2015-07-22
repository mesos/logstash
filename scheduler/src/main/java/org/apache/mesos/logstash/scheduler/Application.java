package org.apache.mesos.logstash.scheduler;

import org.apache.mesos.logstash.state.LiveState;
import org.apache.mesos.logstash.state.LogstashLiveState;
import org.apache.mesos.logstash.state.MockLiveState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static java.util.Arrays.asList;

@SpringBootApplication
@ComponentScan(basePackages = "org.apache.mesos.logstash")
public class Application {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogstashScheduler.class);

    private static String masterURL = null;
<<<<<<< HEAD
    private static boolean isNoCluster = false;
=======
    private static boolean isNoCluster = true;
    private static Path pwd;
>>>>>>> Invert flow in scheduler. No more registering listeners. Listeners get injected.

    protected Application() {
    }

    public static void main(String[] args) throws IOException {

        pwd = Paths.get(".").toAbsolutePath();

        readConfigFromCommandLine(args);

        SpringApplication app = new SpringApplication(Application.class);
        app.setShowBanner(false);

        isNoCluster = asList(args).contains("--no-cluster");

        app.setWebEnvironment(!asList(args).contains("--no-ui"));

        app.run(args);
    }

    @Bean
    @Qualifier("isNoCluster")
    public boolean getNoCluster() {
        return isNoCluster;
    }

    @Bean
    @Qualifier("masterURL")
    public String getMasterURL() {
        return masterURL;
    }

    @Bean
    public Path getConfigRootPath() {
        return pwd.resolve("config/");
    }

    @Bean
    public LiveState getSchedulerStatus() {
        return (isNoCluster) ? new MockLiveState() : new LogstashLiveState();
    }

    private static void readConfigFromCommandLine(String... args) {
        List<String> argList = asList(args);
        int index = argList.indexOf("-m");

        if (index != -1 && argList.size() >= index) {
            masterURL = argList.get(index + 1);
        } else if (getMasterURLFromSystemProps() != null) {
            masterURL = getMasterURLFromSystemProps();
        } else {
            printUsage();
        }

        LOGGER.debug("MasterURL configured. masterUrl={}", masterURL);
    }

    private static String getMasterURLFromSystemProps() {
        // This we need to do since bootRun in gradle does not
        // support commandline arguments.
        return System.getProperty("logstash-mesos.masterUrl");
    }

    private static void printUsage() {
        System.err.println("Usage: logstash-scheduler -m <master-url>");
        System.exit(1);
    }
}
