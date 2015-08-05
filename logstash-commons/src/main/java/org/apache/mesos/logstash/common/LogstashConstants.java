package org.apache.mesos.logstash.common;

public interface LogstashConstants {
    String FRAMEWORK_NAME = "logstash";
    int FAILOVER_TIMEOUT = 300000;
    String EXECUTOR_IMAGE_NAME = "mesos/logstash-executor:0.0.2";
    String NODE_NAME = "logstash.node";
    String TASK_NAME = "logstash.task";
    String TASK_DATE_FORMAT = "yyyyMMdd'T'HHmmss.SSS'Z'";
}
