package org.apache.mesos.logstash.scheduler.mock;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.logstash.scheduler.ClusterStatus;
import org.apache.mesos.logstash.scheduler.ExecutorInfo;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;


public class MockClusterStatus implements ClusterStatus {
    @Override
    public String getId() {
        return "No-Cluster";
    }

    @Override
    public Set<ExecutorInfo> getExecutors() {
        Set<ExecutorInfo> executors = new HashSet<>();
        Random random = new Random();

        ExecutorInfo x = new ExecutorInfo(slaveID("6127-6213-7812-6312"), executorID("1221-1221-64-4345-4"));
        x.setActiveStreamCount(random.nextInt(20) + 10);
        executors.add(x);

        if (random.nextBoolean()) {
            ExecutorInfo e = new ExecutorInfo(slaveID("5555-6213-7812-1275"), executorID("9183-7221-24-3345-1"));
            e.setActiveStreamCount(random.nextInt(30) + 2);
            executors.add(e);
        }

        if (random.nextBoolean()) {
            ExecutorInfo e = new ExecutorInfo(slaveID("1112-3112-8463-1273"), executorID("4113-6222-22-3335-3"));
            e.setActiveStreamCount(random.nextInt(7));
            executors.add(e);
        }

        return executors;
    }

    private ExecutorID executorID(String id) {
        return ExecutorID.newBuilder().setValue(id).build();
    }

    private Protos.SlaveID slaveID(String id) {
        return Protos.SlaveID.newBuilder().setValue(id).build();
    }
}
