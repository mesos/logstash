package org.apache.mesos.logstash.executor.docker;

import org.apache.mesos.logstash.executor.logging.LogStream;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public interface ContainerizerClient {
    Set<String> getRunningContainers();

    String getImageNameOfContainer(String containerId);

    LogStream exec(String containerId, String... command);

    void setDelegate(Consumer<List<String>> consumer);
}