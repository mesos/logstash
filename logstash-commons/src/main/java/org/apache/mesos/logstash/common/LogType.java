package org.apache.mesos.logstash.common;

import java.nio.file.Path;
import java.nio.file.Paths;
public enum LogType {
    DOCKER(Paths.get("docker")),
    HOST(Paths.get("host"));

    private final Path folder;

    LogType(Path folder) {
        this.folder = folder;
    }

    public Path getFolder() {
        return folder;
    }
}