package org.apache.mesos.logstash.executor.frameworks;

/**
 * Framework configuration.
 */
public interface Framework {

    String getName();

    String getConfiguration();
}
