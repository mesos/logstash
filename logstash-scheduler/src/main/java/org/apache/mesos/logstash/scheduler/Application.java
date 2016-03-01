package org.apache.mesos.logstash.scheduler;

import org.apache.commons.lang3.StringUtils;
import org.apache.mesos.logstash.common.network.NetworkUtils;
import org.apache.mesos.logstash.common.zookeeper.formatter.MesosStateZKFormatter;
import org.apache.mesos.logstash.common.zookeeper.formatter.MesosZKFormatter;
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
        new SpringApplication(Application.class).run(args);
    }

    private String getMesosStateZKURL(String zkUrl) {
        ZKFormatter mesosStateZKFormatter = new MesosStateZKFormatter(new ZKAddressParser());
        return mesosStateZKFormatter.format(zkUrl);
    }

    @Bean
    public NetworkUtils networkUtils() {
        return new NetworkUtils();
    }

}
