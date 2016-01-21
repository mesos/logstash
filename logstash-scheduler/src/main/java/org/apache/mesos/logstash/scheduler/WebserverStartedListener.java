package org.apache.mesos.logstash.scheduler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringApplicationRunListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;

/**
 * Listener for when Spring's web server for static content has started
 */
public class WebserverStartedListener implements SpringApplicationRunListener {

    public WebserverStartedListener(SpringApplication application, String[] args) {

    }

    @Override
    public void started() {

    }

    @Override
    public void environmentPrepared(ConfigurableEnvironment environment) {

    }

    @Override
    public void contextPrepared(ConfigurableApplicationContext context) {

    }

    @Override
    public void contextLoaded(ConfigurableApplicationContext context) {

    }

    @Override
    public void finished(ConfigurableApplicationContext context, Throwable exception) {
        LogstashScheduler scheduler = context.getBean(LogstashScheduler.class);
        scheduler.start();
    }

}
