package org.apache.mesos.logstash.executor;

import org.apache.mesos.logstash.common.LogstashProtos;

import java.util.List;

/**
 * Created by peldan on 02/07/15.
 */
public class HostFramework extends Framework {

    public HostFramework(LogstashProtos.LogstashConfig cfg) {
        super(cfg.getFrameworkName(), cfg.getConfig());
    }

    public static HostFramework create(LogstashProtos.LogstashConfig cfg) {
        return new HostFramework(cfg);
    }

    @Override
    protected List<String> parseLogLocations(String configuration) {
        // TODO implement me
        return null;
    }
}
