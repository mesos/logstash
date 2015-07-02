package org.apache.mesos.logstash.executor;

import java.nio.file.Paths;
import java.util.List;

/**
 * Created by peldan on 02/07/15.
 */
public abstract class Framework {
    protected String name;
    protected String configuration;
    protected List<String> logLocations;

    public Framework(String frameworkName, String configuration) {
        this.configuration = configuration;
        this.name = frameworkName;
    }

    public String getName() {
        return name;
    }

    public String getConfiguration() {
        return configuration;
    }

    public List<String> getLogLocations() {
        return logLocations;
    }

    public String generateLogstashConfig(String containerId) {
        String generatedConfiguration = configuration;
        for(String logLocation : logLocations) {
            String localLocation = getLocalLogLocation(containerId, logLocation);
            generatedConfiguration = generatedConfiguration.replace(logLocation, localLocation);
        }
        return generatedConfiguration.replace("docker-path", "path");
    }

    public String getLocalLogLocation(String containerId, String logLocation) {
        String sanitizedFrameworkName = sanitize(name);
        return Paths.get("/tmp", containerId, sanitizedFrameworkName, logLocation).toString();
    }

    private String sanitize(String frameworkName) {
        return frameworkName.replaceFirst(".*/", "").replaceFirst(":\\w+", "");
    }

    protected abstract List<String> parseLogLocations(String configuration);
}
