package org.apache.mesos.logstash.executor.util;

import org.apache.mesos.logstash.executor.docker.ContainerizerClient;
import org.apache.mesos.logstash.executor.frameworks.DockerFramework;
import org.apache.mesos.logstash.executor.frameworks.FrameworkInfo;

import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

public final class ConfigUtil {

    protected ConfigUtil() {
    }

    public static String generateConfigFile(ContainerizerClient client,
        List<FrameworkInfo> dockerInfoList, List<FrameworkInfo> hostInfo) {

        final StringBuilder text = new StringBuilder();

        Map<String, FrameworkInfo> dockerConfigs = dockerInfoList.stream().collect(
            toMap(FrameworkInfo::getName, x -> x));

        client.getRunningContainers().forEach(containerId -> {
            String name = client.getImageNameOfContainer(containerId);

            if (dockerConfigs.containsKey(name)) {
                FrameworkInfo info = dockerConfigs.get(name);

                DockerFramework framework = new DockerFramework(info, new DockerFramework.ContainerId(containerId));
                text.append(framework.getConfiguration()).append("\n");
            }
        });

        hostInfo.forEach(config -> {
            text.append(String.format("# %s\n%s\n", config.getName(), config.getConfiguration()));
        });

        return text.toString();
    }
}
