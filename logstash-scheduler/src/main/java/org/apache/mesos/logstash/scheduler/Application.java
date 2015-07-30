package org.apache.mesos.logstash.scheduler;

import org.apache.mesos.logstash.config.LogstashSettings;
import org.apache.mesos.logstash.state.ILiveState;
import org.apache.mesos.logstash.state.LiveState;
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

    private static LogstashSettings settings;

    protected Application() { }

    public static void main(String[] args) throws IOException {
        settings = new LogstashSettings();
        SpringApplication app = new SpringApplication(Application.class);
        app.setShowBanner(false);
        app.setWebEnvironment(settings.getWebServerEnabled());
        app.run(args);
    }

    @Bean
    public ILiveState getSchedulerStatus() {
        return (settings.getWebServerDebug()) ? new MockLiveState() : new LiveState();
    }

    @Bean
    public JettyEmbeddedServletContainerFactory jettyEmbeddedServletContainerFactory() {
        return new JettyEmbeddedServletContainerFactory(settings.getWebServerPort());
    }
}
