package org.apache.mesos.logstash.scheduler.mock;

import org.apache.mesos.Protos;
import org.apache.mesos.logstash.scheduler.Driver;
import org.apache.mesos.logstash.scheduler.Scheduler;

public class NoopDriver implements Driver {

    private boolean isRunning = false;

    @Override
    public void run(Scheduler scheduler) {
        // Emulate the behaviour of the real Mesos Driver.

        isRunning = true;
        Thread t = new Thread(() -> { while (isRunning) {
            // TODO: Fake events in the cluster.
        }});

        t.start();

        try {
            t.join();
        } catch (InterruptedException ignored) {

        }
    }

    @Override
    public void stop() {
        isRunning = false;
    }

    @Override
    public void sendFrameworkMessage(Protos.ExecutorID value, Protos.SlaveID key, byte[] message) {

    }
}
