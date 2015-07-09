package org.apache.mesos.logstash.executor.frameworks;

public class FrameworkInfo {

    private final String configuration;
    private final String name;

    public FrameworkInfo(String name, String configuration) {
        this.name = name;
        this.configuration = configuration;
    }

    public String getConfiguration() {
        return this.configuration;
    }

    public String getName() {
        return name;
    }

}
