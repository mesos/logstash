package org.apache.mesos.logstash.executor.frameworks;

public class HostFramework implements Framework {

    private FrameworkInfo frameworkInfo;

    public HostFramework(FrameworkInfo frameworkInfo) {
        this.frameworkInfo = frameworkInfo;
    }

    public String getConfiguration() {
        return frameworkInfo.getConfiguration();
    }

    public String getName() {
        return frameworkInfo.getName();
    }

}
