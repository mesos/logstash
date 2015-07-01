package org.apache.mesos.logstash.executor;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by ero on 01/07/15.
 */
public class LogConfigurationTest {

    @Test
    public void testSetLocalLogLocationShouldRemoveSlashAndColon() throws Exception {
        LogConfiguration target = new LogConfiguration("type", "/var/log/logfile.log", "tag");

        target.setLocalLogLocation("account/logstash-executor:latest", "/tmp/containerId");

        assertEquals("/tmp/containerId/logstash-executor/logfile.log", target.getLocalLogLocation());
    }

    @Test
    public void testSetLocalLogLocationShouldHandleFrameworkNameWithoutSlashAndColon() throws Exception {
        LogConfiguration target = new LogConfiguration("type", "/var/log/logfile.log", "tag");

        target.setLocalLogLocation("logstash-executor", "/tmp/containerId");

        assertEquals("/tmp/containerId/logstash-executor/logfile.log", target.getLocalLogLocation());
    }
}