package org.apache.mesos.logstash.config;
import java.nio.file.Path;
import java.nio.file.Paths;


public enum ConfigType {
    DOCKER(Paths.get("docker")),
    HOST(Paths.get("host"));

    private final Path folder;

    ConfigType(Path folder) {
        this.folder = folder;
    }

    public Path getFolder() {
        return folder;
    }
}
