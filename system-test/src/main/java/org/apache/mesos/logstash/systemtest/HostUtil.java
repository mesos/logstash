package org.apache.mesos.logstash.systemtest;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;

import java.io.InputStream;

public class HostUtil {

    private final String mesosLocalContainerID;
    private final DockerClient dockerClient;

    public HostUtil(String mesosLocalContainerID, DockerClient dockerClient) {
        this.mesosLocalContainerID = mesosLocalContainerID;
        this.dockerClient = dockerClient;
    }

    public void createFileWithContent(String dest, String content) {
        writeToFile(">", dest, content);
    }

    private void writeToFile(String op, String dest, String content) {
        ExecCreateCmdResponse execCreateCmdResponse;
        InputStream execCmdStream;

        execCreateCmdResponse = dockerClient
            .execCreateCmd(mesosLocalContainerID)
            .withAttachStdout(true)
            .withCmd("sh", "-c", "echo \"" + content + "\" " + op + " " + dest).exec();

        execCmdStream = dockerClient.execStartCmd(execCreateCmdResponse.getId()).exec();
        System.out.println(ResponseCollector.collectResponse(execCmdStream));
    }
}
