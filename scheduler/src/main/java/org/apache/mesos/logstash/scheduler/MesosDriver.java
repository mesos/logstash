package org.apache.mesos.logstash.scheduler;


import javassist.bytecode.analysis.Executor;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MesosDriver implements Driver {

    private final String masterURL;
    private MesosSchedulerDriver mesosDriver;
    private ExecutorService executorService;


    public MesosDriver(String masterURL) {
        this.masterURL = masterURL;
        this.executorService = Executors.newSingleThreadExecutor();
    }


    @Override
    public synchronized void run(Scheduler scheduler) {
        mesosDriver = buildSchedulerDriver(scheduler);
        executorService.execute(mesosDriver::run);
    }


    @Override
    public void stop() {
        mesosDriver.stop();
        mesosDriver = null;
        executorService.shutdown();
    }


    @Override
    public void sendFrameworkMessage(Protos.ExecutorID value, Protos.SlaveID key, byte[] message) {
        mesosDriver.sendFrameworkMessage(value, key, message);
    }


    private Protos.FrameworkInfo buildFramework() {
        final Protos.FrameworkInfo.Builder frameworkBuilder = Protos.FrameworkInfo.newBuilder();
        frameworkBuilder.setName(Constants.FRAMEWORK_NAME);
        frameworkBuilder.setUser("root"); // TODO: change, (thb) meaning what
        frameworkBuilder.setCheckpoint(true);
        frameworkBuilder.setFailoverTimeout(Constants.FAILOVER_TIMEOUT);
        return frameworkBuilder.build();
    }


    private MesosSchedulerDriver buildSchedulerDriver(Scheduler scheduler) {
        final Protos.FrameworkInfo frameworkInfo = buildFramework();

        return new MesosSchedulerDriver(scheduler, frameworkInfo, masterURL);
    }
}
