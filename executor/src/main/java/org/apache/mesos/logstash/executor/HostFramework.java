package org.apache.mesos.logstash.executor;

import org.apache.mesos.logstash.common.LogstashProtos;

import java.util.List;

/**
 * Created by peldan on 02/07/15.
 */
public class HostFramework extends Framework {

    public HostFramework(LogstashInfo logstashInfo) {
        super(logstashInfo.getName(), logstashInfo.getConfiguration());
    }

    @Override
    public String generateLogstashConfig() {
        // TODO implement me
        return null;
    }

    @Override
    protected List<String> parseLogLocations(String configuration) {
        // TODO implement me
        return null;
    }
}
