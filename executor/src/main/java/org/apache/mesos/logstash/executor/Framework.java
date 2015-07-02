package org.apache.mesos.logstash.executor;

import org.apache.mesos.logstash.common.LogstashProtos;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by ero on 22/06/15.
 */
public class Framework {

    public String getName() {
        return name;
    }

    public String getConfiguration() {
        return configuration;
    }

    public List<String> getLogLocations() {
        return logLocations;
    }

    private String name;
    private String configuration;
    private List<String> logLocations;

    public Framework(LogstashProtos.LogstashConfig logstashConfig) {
        this(logstashConfig.getFrameworkName(), logstashConfig.getConfig());
    }

    public Framework(String frameworkName, String configuration) {
        this.name = frameworkName;
        this.configuration = configuration;
        this.logLocations = parseLogLocations(configuration);
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

    private List<String> parseLogLocations(String configuration) {
        List<String> locations = new ArrayList<>();
        Pattern pattern = Pattern.compile("docker-path\\s*=>\\s*\"([^}   ]+)\"");

        Matcher matcher = pattern.matcher(configuration);

        while(matcher.find()) {
            locations.add(matcher.group(1));
        }

        return locations;
    }
}
