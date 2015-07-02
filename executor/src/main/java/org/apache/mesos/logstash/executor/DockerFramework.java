package org.apache.mesos.logstash.executor;

import org.apache.mesos.logstash.common.LogstashProtos;

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

    static DockerFramework create(LogstashProtos.LogstashConfig logstashConfig) {
        return new DockerFramework(logstashConfig);
    }
}
