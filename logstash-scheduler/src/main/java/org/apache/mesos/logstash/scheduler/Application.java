package org.apache.mesos.logstash.scheduler;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.mesos.logstash.common.zookeeper.formatter.MesosStateZKFormatter;
import org.apache.mesos.logstash.common.zookeeper.formatter.MesosZKFormatter;
import org.apache.mesos.logstash.common.zookeeper.formatter.ZKFormatter;
import org.apache.mesos.logstash.common.zookeeper.parser.ZKAddressParser;
import org.apache.mesos.logstash.config.Configuration;
import org.apache.mesos.logstash.config.LogstashSystemProperties;
import org.apache.mesos.logstash.state.LiveState;
import org.apache.mesos.logstash.state.SerializableState;
import org.apache.mesos.logstash.state.SerializableZookeeperState;
import org.apache.mesos.state.ZooKeeperState;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.embedded.jetty.JettyEmbeddedServletContainerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@ComponentScan(basePackages = {"org.apache.mesos.logstash"})
public class Application {

    private LogstashSystemProperties logstashSystemProperties = new LogstashSystemProperties();
//    private Logger LOGGER = Logger.getLogger(Application.class.getCanonicalName());

    public static void main(String[] args) throws IOException {
        Application app = new Application();
        app.run(args);
    }

    private void run(String[] args) {
        LogstashSystemProperties settings = new LogstashSystemProperties();
        checkSystemProperties(settings);

        SpringApplication app = new SpringApplication(Application.class);
        app.setShowBanner(false);
        app.setWebEnvironment(settings.getWebServerEnabled());
        app.run(args);
    }

    @Bean
    public Configuration getLogstashConfiguration() throws UnknownHostException {
        SimpleFileServer fs = new SimpleFileServer();
        fs.run();
        Configuration conf = new Configuration();
        conf.setVolumeString(logstashSystemProperties.getVolumes());
        conf.setState(getState(logstashSystemProperties));
        conf.setSchedulerHttpEndpoint("http://" + fs.getAddress().getAddress().getHostAddress() + ":" + "9093");
        conf.setZookeeperUrl(getMesosZKURL(logstashSystemProperties.getZookeeperServerProperty()));
        conf.setExecutorCpus(logstashSystemProperties.getExecutorCpus());
        conf.setExecutorHeapSize(logstashSystemProperties.getExecutorHeapSize());
        conf.setLogstashHeapSize(logstashSystemProperties.getLogstashHeapSize());
        conf.setFailoverTimeout(logstashSystemProperties.getFailoverTimeout());
        conf.setFrameworkName(logstashSystemProperties.getFrameworkName());
        conf.setLogStashRole(logstashSystemProperties.getLogstashRole());
        conf.setLogStashUser(logstashSystemProperties.getLogstashUser());
        conf.setZkTimout(logstashSystemProperties.getZkTimeout());
        conf.setWebServerPort(logstashSystemProperties.getWebServerPort());

        return conf;
    }

    @Bean
    public LiveState getSchedulerStatus() {
        return new LiveState();
    }

    @Bean
    public JettyEmbeddedServletContainerFactory jettyEmbeddedServletContainerFactory() {
        return new JettyEmbeddedServletContainerFactory(
            logstashSystemProperties.getWebServerPort());
    }

    private void checkSystemProperties(LogstashSystemProperties settings) {
        if (StringUtils.isEmpty(settings.getZookeeperServerProperty())) {
            System.out.println(
                "No zookeeper configuration given. Please provide \"mesos.zk\" system property.");
            System.exit(2);
        }

        // will throw an IllegalArgumentException if the URI is not parseable
        getMesosZKURL(settings.getZookeeperServerProperty());
        getMesosStateZKURL(settings.getZookeeperServerProperty());
    }

    private String getMesosStateZKURL(String zkUrl) {
        ZKFormatter mesosStateZKFormatter = new MesosStateZKFormatter(new ZKAddressParser());
        return mesosStateZKFormatter.format(zkUrl);
    }

    private static String getMesosZKURL(String zkUrl) {
        ZKFormatter mesosZKFormatter = new MesosZKFormatter(new ZKAddressParser());
        return mesosZKFormatter.format(zkUrl);
    }

    private SerializableState getState(LogstashSystemProperties settings) {
        String zkUrl = settings.getZookeeperServerProperty();
        String frameworkName = settings.getFrameworkName();
        if (!frameworkName.startsWith("/")){
            frameworkName = "/" + frameworkName; // znode must start with a slash
        }
        org.apache.mesos.state.State state = new ZooKeeperState(
            getMesosStateZKURL(zkUrl),
            settings.getZkTimeout(),
            TimeUnit.MILLISECONDS,
            frameworkName);
        return new SerializableZookeeperState(state);
    }
}
