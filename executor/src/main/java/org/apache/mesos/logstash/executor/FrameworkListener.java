package org.apache.mesos.logstash.executor;

/**
 * Created by ero on 22/06/15.
 */
public interface FrameworkListener {
    void frameworkAdded(Framework f);
    void frameworkRemoved(Framework f);
}
