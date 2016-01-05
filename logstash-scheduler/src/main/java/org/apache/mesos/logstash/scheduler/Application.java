package org.apache.mesos.logstash.scheduler;

import org.apache.commons.lang.StringUtils;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.logstash.common.zookeeper.formatter.MesosStateZKFormatter;
import org.apache.mesos.logstash.common.zookeeper.formatter.MesosZKFormatter;
import org.apache.mesos.logstash.common.zookeeper.formatter.ZKFormatter;
import org.apache.mesos.logstash.common.zookeeper.parser.ZKAddressParser;
import org.apache.mesos.logstash.config.FrameworkConfig;
import org.apache.mesos.state.State;
import org.apache.mesos.state.ZooKeeperState;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.embedded.jetty.JettyEmbeddedServletContainerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import javax.inject.Inject;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Configuration
@EnableAutoConfiguration
@ComponentScan(basePackages = {"org.apache.mesos.logstash"})
@EnableConfigurationProperties
public class Application {
    @Inject
    Features features;
    @Inject
    FrameworkConfig frameworkConfig;

    public static void main(String[] args) throws IOException {
        Application app = new Application();
        app.run(args);
    }

    private void run(String[] args) {
//        checkSystemProperties(zkUrl);

        SpringApplication app = new SpringApplication(Application.class);
        app.setShowBanner(false);
        app.run(args);
    }

    private void checkSystemProperties(String zkUrl) {
        if (StringUtils.isEmpty(zkUrl)) {
            System.out.println(
                "No zookeeper configuration given. Please provide \"mesos.zk\" system property.");
            System.exit(2);
        }

        // will throw an IllegalArgumentException if the URI is not parseable
        getMesosZKURL(zkUrl);
        getMesosStateZKURL(zkUrl);
    }

    private String getMesosStateZKURL(String zkUrl) {
        ZKFormatter mesosStateZKFormatter = new MesosStateZKFormatter(new ZKAddressParser());
        return mesosStateZKFormatter.format(zkUrl);
    }

    private static String getMesosZKURL(String zkUrl) {
        ZKFormatter mesosZKFormatter = new MesosZKFormatter(new ZKAddressParser());
        return mesosZKFormatter.format(zkUrl);
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public SchedulerDriver driver(LogstashScheduler scheduler, Protos.FrameworkInfo framework, String master) {
        return new MesosSchedulerDriver(scheduler, framework, master);
    }

    @Bean
    public State zkState(FrameworkConfig frameworkConfig) {
        String frameworkName = frameworkConfig.getFrameworkName();
        if (!frameworkName.startsWith("/")) {
            frameworkName = "/" + frameworkName; // znode must start with a slash
        }
        return new ZooKeeperState(
                getMesosStateZKURL(frameworkConfig.getZkUrl()),
                frameworkConfig.getZkTimeout(),
                TimeUnit.MILLISECONDS,
                frameworkName);

    }
}
