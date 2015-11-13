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

    public String getPsAuxOutput(){

        ExecCreateCmdResponse execCreateCmdResponse;

        execCreateCmdResponse = dockerClient
            .execCreateCmd(getContainerId())
            .withAttachStdout(true)
            .withCmd("ps", "aux").exec();

        final ExecCreateCmdResponse finalExecCreateCmdResponse = execCreateCmdResponse;

        return ResponseCollector.collectResponse(dockerClient
            .execStartCmd(finalExecCreateCmdResponse.getId()).exec());
    }


    public void createFileWithContent(String dest, String content) {
        writeToFile(">", dest, content);
    }

    public void appendContentToFile(String dest, String content) {
        writeToFile(">>", dest, content);
    }

    private void writeToFile(String op, String dest, String content) {
        ExecCreateCmdResponse execCreateCmdResponse;
        InputStream execCmdStream;

        execCreateCmdResponse = dockerClient
            .execCreateCmd(getContainerId())
            .withAttachStdout(true)
            .withCmd("sh", "-c", "echo \"" + content + "\" " + op + " " + dest).exec();

        execCmdStream = dockerClient.execStartCmd(execCreateCmdResponse.getId()).exec();
        System.out.println(ResponseCollector.collectResponse(execCmdStream));
    }
}
