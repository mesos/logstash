package org.apache.mesos.logstash.scheduler;

import org.apache.mesos.logstash.config.LogstashSettings;
import org.apache.mesos.logstash.state.LiveState;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.embedded.jetty.JettyEmbeddedServletContainerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

import java.io.IOException;

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
    public LiveState getSchedulerStatus() {
        return new LiveState();
    }

    @Bean
    public JettyEmbeddedServletContainerFactory jettyEmbeddedServletContainerFactory() {
        return new JettyEmbeddedServletContainerFactory(settings.getWebServerPort());
    }
}
