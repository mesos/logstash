package org.apache.mesos.logstash.scheduler;


import org.apache.mesos.Protos;

public interface Driver {
    void run(Scheduler scheduler);
    void stop();
    void sendFrameworkMessage(Protos.ExecutorID value, Protos.SlaveID key, byte[] message);
}
