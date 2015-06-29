package org.apache.mesos.logstash.executor;

import java.util.Map;

/**
 * Created by peldan on 26/06/15.
 */
public interface LogConfigurationListener {
    void updatedLogLocations(Map<String, String[]> locationsPerFrameworkName);
}
