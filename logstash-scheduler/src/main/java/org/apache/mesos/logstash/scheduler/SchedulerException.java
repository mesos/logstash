package org.apache.mesos.logstash.scheduler;
public class SchedulerException extends RuntimeException {
    public SchedulerException(String msg, Throwable e) {
        super(msg, e);
    }
}
