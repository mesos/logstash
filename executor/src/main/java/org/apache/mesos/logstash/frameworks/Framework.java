package org.apache.mesos.logstash.frameworks;

import java.nio.file.Paths;
import java.util.List;

/**
 * Created by peldan on 02/07/15.
 */
public abstract class Framework {
    protected String name;
    protected String configuration;

    public Framework(String frameworkName, String configuration) {
        this.configuration = configuration;
        this.name = frameworkName;
    }

    public String getName() {
        return name;
    }

    public String getConfiguration() {
        return configuration;
    }

    public abstract String generateLogstashConfig();
}
