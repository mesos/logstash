package org.apache.mesos.logstash.executor;

/**
 * Created by ero on 22/06/15.
 */
public interface FrameworkListener {
    void FrameworkAdded(Framework f);
    void FrameworkRemoved(Framework f);
}
