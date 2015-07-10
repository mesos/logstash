package org.apache.mesos.logstash.executor.docker;


import java.nio.file.Paths;

public class DockerLogPath {

    private final String executorLogPath;
    private final String containerFilePath;
    private final String containerId;

    public DockerLogPath(String containerId, String frameworkName, String containerFilePath) {
        this.containerId = containerId;
        String sanitizedFrameworkName = sanitize(frameworkName);
        this.containerFilePath = containerFilePath;
        this.executorLogPath = Paths.get("/tmp", containerId, sanitizedFrameworkName, containerFilePath).toString();
    }

    public String getContainerLogPath() {
        return containerFilePath;
    }

    public String getExecutorLogPath() {
        return executorLogPath;
    }

    private static String sanitize(String frameworkName) {
        return frameworkName.replaceFirst(".*/", "").replaceFirst(":\\w+", "");
    }

    public String getContainerId() {
        return containerId;
    }
}
