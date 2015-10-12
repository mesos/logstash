package org.apache.mesos.logstash.systemtest;
import com.containersol.minimesos.docker.ResponseCollector;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.model.Container;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.await;


public class LogstashExecutorContainer {

    final DockerClient dockerClient;
    private final String executorId;

    public LogstashExecutorContainer(DockerClient dockerClient) {
        this.dockerClient = dockerClient;

        await().atMost(60, TimeUnit.SECONDS).until(
            () -> dockerClient.listContainersCmd().exec().size() > 0);
        List<Container> containers = dockerClient.listContainersCmd().exec();

        String id = null;
        // Find a single executor container
        for (Container container : containers) {
            if (container.getImage().contains("executor")) {
                id = container.getId();
                break;
            }
        }

        executorId = id;
    }

    public String getContentOfFile(String path){
        ExecCreateCmdResponse execCreateCmdResponse;

        execCreateCmdResponse = dockerClient
            .execCreateCmd(executorId)
            .withAttachStdout(true)
            .withCmd("cat", path).exec();

        final ExecCreateCmdResponse finalExecCreateCmdResponse = execCreateCmdResponse;

        return ResponseCollector.collectResponse(dockerClient
                .execStartCmd(finalExecCreateCmdResponse.getId()).exec());
    }





}
