package org.apache.mesos.logstash.executor.docker;

import java.nio.file.Paths;

public class DockerLogPath {

    private final String executorLogPath;
    private final String containerFilePath;
    private final String containerId;
    private final String frameworkName;

    public DockerLogPath(String containerId, String frameworkName, String containerFilePath) {
        this.containerId = containerId;
        String sanitizedFrameworkName = sanitize(frameworkName);
        this.frameworkName = frameworkName;
        this.containerFilePath = containerFilePath;
        this.executorLogPath = Paths
            .get("/tmp", containerId, sanitizedFrameworkName, containerFilePath).toString();
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

    @Override public boolean equals(Object obj) {

        if (obj instanceof DockerLogPath) {
            DockerLogPath other = (DockerLogPath) obj;

            return executorLogPath.equals(other.executorLogPath) &&
                containerFilePath.equals(other.containerFilePath) &&
                containerId.equals(other.containerId);

        }
        return false;
    }

    @Override public int hashCode() {
        return (containerId + executorLogPath + containerFilePath).hashCode();
    }

    @Override public String toString() {
        return String.format("DockerLogPath: Framework %s (ContainerID %s) - path: %s", frameworkName, containerId, containerFilePath);
    }
}
