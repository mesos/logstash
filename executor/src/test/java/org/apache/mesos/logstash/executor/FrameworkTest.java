package org.apache.mesos.logstash.executor;

import org.apache.mesos.logstash.frameworks.DockerFramework;
import org.apache.mesos.logstash.frameworks.Framework;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by ero on 02/07/15.
 */
public class FrameworkTest {

    @Test
    public void testFrameworkExpectingLogLocationsToBeFound() {
        String frameworkName = "TEST_FRAMEWORK_NAME";
        String configuration =
                "input { " +
                        "file { docker-path => \"/var/log/nginx/logs.log\" } " +
                        "file { docker-path => \"/var/log/mysql/logs.log\" } " +
                        "}";

        Framework target = new DockerFramework(frameworkName, configuration);

        assertEquals("/var/log/nginx/logs.log", target.getLogLocations().get(0));
        assertEquals("/var/log/mysql/logs.log", target.getLogLocations().get(1));
    }

    @Test
    public void testGetLocalLogLocation() throws Exception {
        String logLocation = "/var/log/nginx/logs.log";

    }

    @Test
    public void testGenerateLogstashConfig() throws Exception {
        String frameworkName = "TEST_FRAMEWORK_NAME";
        String configuration =
                "input { " +
                        "file { docker-path => \"/var/log/nginx/logs.log\" } " +
                        "file { docker-path => \"/var/log/mysql/logs.log\" } " +
                        "}";
        String expectedGenareatedConfiguration =
                "input { " +
                        "file { path => \"/tmp/TEST_CONTAINER_ID/TEST_FRAMEWORK_NAME/var/log/nginx/logs.log\" } " +
                        "file { path => \"/tmp/TEST_CONTAINER_ID/TEST_FRAMEWORK_NAME/var/log/mysql/logs.log\" } " +
                        "}";

        Framework target = new DockerFramework(frameworkName, configuration);
        String result = target.generateLogstashConfig("TEST_CONTAINER_ID");

        assertEquals(expectedGenareatedConfiguration, result);
    }
}