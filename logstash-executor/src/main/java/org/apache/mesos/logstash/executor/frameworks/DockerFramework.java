package org.apache.mesos.logstash.executor.frameworks;

import org.apache.mesos.logstash.common.LogstashProtos.LogstashConfig;

/**
 * Docker configuration for the framework.
 */
public class DockerFramework implements Framework {

    public final LogstashConfig frameworkInfo;

    public DockerFramework(LogstashConfig frameworkInfo) {
        this.frameworkInfo = frameworkInfo;
    }
}
