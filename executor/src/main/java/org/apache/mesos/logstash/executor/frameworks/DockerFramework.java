package org.apache.mesos.logstash.executor.frameworks;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DockerFramework implements Framework {

    private final ContainerId containerId;
    private final FrameworkInfo frameworkInfo;
    private List<String> logLocations;

    public DockerFramework(FrameworkInfo frameworkInfo, ContainerId containerId) {
        this.frameworkInfo = frameworkInfo;

        this.containerId = containerId;
        this.logLocations = parseLogLocations(frameworkInfo.getConfiguration());
    }

    public String getContainerId() {
        return this.containerId.id;
    }

    /**
     * Produces a valid logstash configuration from a (very similar looking) Framework configuration
     */
    @Override
    public String getConfiguration() {
        String generatedConfiguration = frameworkInfo.getConfiguration();

        // Replace all log paths with paths to temporary files
        for (String logLocation : logLocations) {
            String localLocation = getLocalLogLocation(logLocation);
            generatedConfiguration = generatedConfiguration.replace(logLocation, localLocation);
        }

        // replace 'magic' string docker-path with normal path string
        return generatedConfiguration.replace("docker-path", "path");
    }

    public String getLocalLogLocation(String logLocation) {
        String sanitizedFrameworkName = sanitize(frameworkInfo.getName());
        return Paths.get("/tmp", containerId.id, sanitizedFrameworkName, logLocation).toString();
    }

    private List<String> parseLogLocations(String configuration) {
        List<String> locations = new ArrayList<>();
        Pattern pattern = Pattern.compile("docker-path\\s*=>\\s*\"([^}\\s]+)\"");

        Matcher matcher = pattern.matcher(configuration);

        while (matcher.find()) {
            locations.add(matcher.group(1));
        }

        return locations;
    }

    private String sanitize(String frameworkName) {
        return frameworkName.replaceFirst(".*/", "").replaceFirst(":\\w+", "");
    }

    public static class ContainerId {
        String id;

        public ContainerId(String containerId) {
            this.id = containerId;
        }
    }

    public List<String> getLogLocations() {
        return logLocations;
    }

    public String getName() {
        return frameworkInfo.getName();
    }
}
