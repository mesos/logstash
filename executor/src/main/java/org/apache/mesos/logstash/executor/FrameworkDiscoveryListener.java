package org.apache.mesos.logstash.executor;

import java.util.List;

/**
 * Created by ero on 26/06/15.
 */
public interface FrameworkDiscoveryListener {
    void frameworksDiscovered(List<String> frameworkNames);
}
