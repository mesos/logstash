package org.apache.mesos.logstash.executor;

import java.util.List;

/**
 * Created by peldan on 26/06/15.
 */
public interface LogConfigurationListener {
    void updatedLogLocations(List<Framework> frameworks);
}
