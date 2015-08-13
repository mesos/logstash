package org.apache.mesos.logstash.common;

public interface LogstashConstants {
    String FRAMEWORK_NAME = "logstash";
    int FAILOVER_TIMEOUT = 300000;
    String EXECUTOR_IMAGE_NAME = "mesos/logstash-executor";
    String EXECUTOR_IMAGE_TAG = "0.0.6";
    String EXECUTOR_IMAGE_NAME_WITH_TAG = EXECUTOR_IMAGE_NAME + ":" + EXECUTOR_IMAGE_TAG;
    String NODE_NAME = "logstash.node";
    String TASK_NAME = "logstash.task";
    String TASK_DATE_FORMAT = "yyyyMMdd'T'HHmmss.SSS'Z'";
    String VOLUME_MOUNT_DIR = "/tmp/volumes/";
}
