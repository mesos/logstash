package org.apache.mesos.logstash.scheduler;

import java.util.Set;

public interface ClusterStatus {
    String getId();
    Set<ExecutorInfo> getExecutors();
}
