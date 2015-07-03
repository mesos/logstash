package org.apache.mesos.logstash.frameworks;

import org.apache.mesos.logstash.LogstashInfo;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by ero on 22/06/15.
 */
public class DockerFramework extends Framework {

    private final ContainerId containerId;

    public DockerFramework(LogstashInfo logstashInfo, ContainerId containerId) {
        super(logstashInfo.getName(), logstashInfo.getConfiguration());
        this.containerId = containerId;
    }

    public String getContainerId() {
        return this.containerId.id;
    }

    /**
     * Produces a valid logstash configuration from a (very similar looking) Framework configuration
     * @return
     */
    @Override
    public String generateLogstashConfig() {
        String generatedConfiguration = configuration;

        // Replace all log paths with paths to temporary files
        for(String logLocation : logLocations) {
            String localLocation = getLocalLogLocation(logLocation);
            generatedConfiguration = generatedConfiguration.replace(logLocation, localLocation);
        }

        // replace 'magic' string docker-path with normal path string
        return generatedConfiguration.replace("docker-path", "path");
    }

    public String getLocalLogLocation(String logLocation) {
        String sanitizedFrameworkName = sanitize(name);
        return Paths.get("/tmp", containerId.id, sanitizedFrameworkName, logLocation).toString();
    }

    @Override
    protected List<String> parseLogLocations(String configuration) {
        List<String> locations = new ArrayList<>();
        Pattern pattern = Pattern.compile("docker-path\\s*=>\\s*\"([^}   ]+)\"");

        Matcher matcher = pattern.matcher(configuration);

        while(matcher.find()) {
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
}
