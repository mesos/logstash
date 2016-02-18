package org.apache.mesos.logstash.systemtest;
import com.containersol.minimesos.container.AbstractContainer;
import com.containersol.minimesos.docker.ResponseCollector;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;

import java.io.InputStream;

public class DummyFrameworkContainer extends AbstractContainer {

    public static final String BUSYBOX_IMAGE = "busybox";
    private static final String TAG = "latest";
    private final String name;

    public DummyFrameworkContainer(DockerClient dockerClient, String name) {
        super(dockerClient);
        this.name = name;
    }

    @Override protected void pullImage() {
        pullImage(BUSYBOX_IMAGE, TAG);
    }

    @Override protected CreateContainerCmd dockerCommand() {

        return dockerClient
            .createContainerCmd(BUSYBOX_IMAGE + ":" + TAG)
            .withName(name)
            .withTty(true)
            .withCmd("sh");
    }

    @Override
    public String getRole() {
        return name;
    }
}
