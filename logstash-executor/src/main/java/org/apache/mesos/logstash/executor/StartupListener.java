package org.apache.mesos.logstash.executor;

/**
 * Created by peldan on 17/07/15.
 */
public interface StartupListener {
    void startupComplete(String hostName);
}
