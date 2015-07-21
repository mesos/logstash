package org.apache.mesos.logstash.scheduler;

import org.apache.mesos.logstash.scheduler.mock.MockClusterStatus;
import org.apache.mesos.logstash.scheduler.mock.NoopDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;

@SpringBootApplication
public class Application {

    private static final Logger LOGGER = LoggerFactory.getLogger(Scheduler.class);

    private static String masterURL = null;
    private static boolean isNoCluster = false;

    protected Application() {}

    public static void main(String[] args) throws IOException {

        readConfigFromCommandLine(args);

        SpringApplication app = new SpringApplication(Application.class);
        app.setShowBanner(false);

        if (asList(args).contains("--no-cluster")) isNoCluster = true;
        if (asList(args).contains("--no-ui")) app.setWebEnvironment(false);

        app.run(args);
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
    public Driver getMesosDriver() {
        if (isNoCluster) return new NoopDriver();
        return new MesosDriver(masterURL);
    }

    @Bean
    public ClusterStatus getSchedulerStatus(Scheduler scheduler) {
        if (isNoCluster) return new MockClusterStatus();
        return new MesosClusterStatus(scheduler);
    }

    private static void readConfigFromCommandLine(String... args) {
        List<String> argList = asList(args);
        int index = argList.indexOf("-m");

        if (index != -1 && argList.size() >= index) {
            masterURL = argList.get(index + 1);
            LOGGER.debug("MasterURL configured. masterUrl={}", masterURL);
        }
        else if (getMasterURLFromSystemProps() != null) {
            masterURL = getMasterURLFromSystemProps();
        }
        else {
            printUsage();
        }
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
