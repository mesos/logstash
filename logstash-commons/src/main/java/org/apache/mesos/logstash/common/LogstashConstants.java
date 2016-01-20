package org.apache.mesos.logstash.common;

/**
 * Framework constants.
 */
public interface LogstashConstants {
    String FRAMEWORK_NAME = "logstash";
    String NODE_NAME = "logstash.node";
    String TASK_NAME = "logstash.task";
    String TASK_DATE_FORMAT = "yyyyMMdd'T'HHmmss.SSS'Z'";
}
