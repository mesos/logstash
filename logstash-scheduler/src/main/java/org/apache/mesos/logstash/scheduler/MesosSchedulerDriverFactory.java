package org.apache.mesos.logstash.scheduler;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.springframework.stereotype.Component;


@Component
public class MesosSchedulerDriverFactory {
    public SchedulerDriver createMesosDriver(Scheduler scheduler, Protos.FrameworkInfo frameworkInfo, String zookeeperURL) {
        return new MesosSchedulerDriver(scheduler, frameworkInfo, zookeeperURL);
    }

    public SchedulerDriver createMesosDriver(Scheduler scheduler, Protos.FrameworkInfo frameworkInfo, Protos.Credential credential, String zookeeperURL) {
        return new MesosSchedulerDriver(scheduler, frameworkInfo, zookeeperURL, credential);
    }
}
