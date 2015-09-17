package org.apache.mesos.logstash.common.zookeeper.formatter;

/**
 * Interface for formatters.
 */
public interface ZKFormatter {
    String format(String zkURL);
}
