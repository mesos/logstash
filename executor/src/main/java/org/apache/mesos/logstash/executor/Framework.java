package org.apache.mesos.logstash.executor;

import org.apache.mesos.logstash.common.LogstashProtos;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ero on 22/06/15.
 */
public class Framework {

    public String getId() {
        return id;
    }

    public List<LogConfiguration> getLogConfigurationList() {
        return logConfigurationList;
    }

    private String id;
    private List<LogConfiguration> logConfigurationList;

    public Framework(LogstashProtos.LogstashConfig logstashConfig) {
        this.id = logstashConfig.getFrameworkName();
        this.logConfigurationList = createLogConfigurations(logstashConfig);
    }

    public Framework(String frameworkName, List<LogConfiguration> logConfigurations) {
        this.id = frameworkName;
        this.logConfigurationList = logConfigurations;
    }

    public List<String> getLocations() {
        List<String> locations = new ArrayList<>();
        for(LogConfiguration lc : this.logConfigurationList) {
            locations.add(lc.getLogLocation());
        }

        return locations;
    }

    public void setLocalLogLocation(String localLogLocation) {
        for(LogConfiguration logConfiguration : logConfigurationList) {
            logConfiguration.setLocalLogLocation(this.id, localLogLocation);
        }
    }

    private List<LogConfiguration> createLogConfigurations(LogstashProtos.LogstashConfig logstashConfig) {
        List<LogConfiguration> logConfigurations = new ArrayList<>();

        for(LogstashProtos.LogInputConfiguration logInputConfiguration : logstashConfig.getLogInputConfiguratonList()) {
            logConfigurations.add(new LogConfiguration(logInputConfiguration));
        }
        return logConfigurations;
    }
}
