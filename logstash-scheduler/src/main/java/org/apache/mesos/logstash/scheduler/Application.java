package org.apache.mesos.logstash.scheduler;

import org.apache.commons.lang3.StringUtils;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.logstash.common.zookeeper.formatter.MesosStateZKFormatter;
import org.apache.mesos.logstash.common.zookeeper.formatter.ZKFormatter;
import org.apache.mesos.logstash.common.zookeeper.parser.ZKAddressParser;
import org.apache.mesos.logstash.config.FrameworkConfig;
import org.apache.mesos.state.State;
import org.apache.mesos.state.ZooKeeperState;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
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
        SpringApplication app = new SpringApplication(Application.class);
        app.setShowBanner(false);
        app.run(args);
    }

    private String getMesosStateZKURL(String zkUrl) {
        ZKFormatter mesosStateZKFormatter = new MesosStateZKFormatter(new ZKAddressParser());
        return mesosStateZKFormatter.format(zkUrl);
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
