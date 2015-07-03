package org.apache.mesos.logstash;

import java.util.stream.Stream;

/**
 * Created by peldan on 26/06/15.
 */
public interface LogConfigurationListener {
    void updatedDockerLogConfigurations(Stream<LogstashInfo> frameworks);
    void updatedHostLogConfigurations(Stream<LogstashInfo> frameworks);

}
