package org.apache.mesos.logstash.scheduler;


import org.springframework.beans.factory.annotation.Autowired;

import java.util.Set;

public class MesosClusterStatus implements ClusterStatus {

    private final Scheduler scheduler;

    @Autowired
    MesosClusterStatus(Scheduler scheduler) {

        this.scheduler = scheduler;
    }

    @Override
    public String getId() {
        return scheduler.getId();
    }

    @Override
    public Set<ExecutorInfo> getExecutors() {
        return scheduler.getExecutors();
    }
}
