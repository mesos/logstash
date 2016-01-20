package org.apache.mesos.logstash.systemtest;

import com.containersol.minimesos.mesos.MesosMaster;
import com.containersol.minimesos.mesos.ZooKeeper;
import com.github.dockerjava.api.DockerClient;

import java.util.TreeMap;

public class LogstashMesosMaster extends MesosMaster {
    public LogstashMesosMaster(DockerClient dockerClient, ZooKeeper zooKeeperContainer) {
        super(dockerClient, zooKeeperContainer);
    }

    @Override
    public TreeMap<String, String> getDefaultEnvVars() {
        final TreeMap<String, String> vars = super.getDefaultEnvVars();
        vars.put("MESOS_ROLES", "logstash");
        return vars;
    }
}
