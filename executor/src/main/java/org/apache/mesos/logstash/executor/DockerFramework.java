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
public class DockerFramework extends Framework {

    public DockerFramework(LogstashProtos.LogstashConfig logstashConfig) {
        super(logstashConfig.getFrameworkName(), logstashConfig.getConfig());
    }

    /**
     * Produces a valid logstash configuration from a (very similar looking) Framework configuration
     * @param containerId
     * @return
     */
    public String generateLogstashConfig(String containerId) {
        String generatedConfiguration = configuration;

        // Replace all log paths with paths to temporary files
        for(String logLocation : logLocations) {
            String localLocation = getLocalLogLocation(containerId, logLocation);
            generatedConfiguration = generatedConfiguration.replace(logLocation, localLocation);
        }

        // replace 'magic' string docker-path with normal path string
        return generatedConfiguration.replace("docker-path", "path");
    }

    public String getLocalLogLocation(String containerId, String logLocation) {
        String sanitizedFrameworkName = sanitize(name);
        return Paths.get("/tmp", containerId, sanitizedFrameworkName, logLocation).toString();
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

    public static DockerFramework create(LogstashProtos.LogstashConfig logstashConfig) {
        return new DockerFramework(logstashConfig);
    }

    private String sanitize(String frameworkName) {
        return frameworkName.replaceFirst(".*/", "").replaceFirst(":\\w+", "");
    }
}
