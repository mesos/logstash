package org.apache.mesos.logstash.executor.util;

import org.apache.mesos.logstash.common.LogstashConstants;
import org.apache.mesos.logstash.common.LogstashProtos.LogstashConfig;
import org.apache.mesos.logstash.executor.docker.DockerClient;
import org.apache.mesos.logstash.executor.frameworks.DockerFramework;

import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

public final class ConfigUtil {

    protected ConfigUtil() {
    }

    public static String generateConfigFile(DockerClient client,
        List<LogstashConfig> dockerInfo, List<LogstashConfig> hostInfo) {

        final StringBuilder text = new StringBuilder();

        Map<String, LogstashConfig> dockerConfigs = dockerInfo.stream().collect(
            toMap(LogstashConfig::getFrameworkName, x -> x));

        client.getRunningContainers().forEach(containerId -> {
            String name = client.getImageNameOfContainer(containerId);

            if (dockerConfigs.containsKey(name)) {
                LogstashConfig info = dockerConfigs.get(name);

                DockerFramework framework = new DockerFramework(info, new DockerFramework.ContainerId(containerId));
                text.append(framework.getConfiguration()).append("\n");
            }
        });

        hostInfo.forEach(config -> {
            text.append(String.format("# %s\n%s\n", config.getFrameworkName(), updateHostPaths(config.getConfig())));
        });

        return text.toString();
    }

    static String updateHostPaths(String configuration) {
        return configuration.replaceAll("\"?host-path\"?\\s*=>\\s*\"([^}\\s]+)\"", "\"path\" => \"" + LogstashConstants.VOLUME_MOUNT_DIR + "$1\"");
    }
}
