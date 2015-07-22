package org.apache.mesos.logstash.systemtest;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.docker.DockerUtil;
import org.apache.mesos.mini.mesos.MesosClusterConfig;
import org.hamcrest.Matchers;

import java.io.InputStream;

import static org.junit.Assert.assertThat;

public class LocalCluster {

    public final MesosClusterConfig clusterConfig = MesosClusterConfig.builder()
        .numberOfSlaves(1)
        .privateRegistryPort(3333)
        .proxyPort(12345)
        .slaveResources(new String[]{"ports(*):[9299-9299,9300-9300]"})
        .build();

    private LocalMesosCluster cluster = new LocalMesosCluster(clusterConfig);

    public static void main(String[] args) throws Exception {
        new LocalCluster().run();
    }

    private void run() throws Exception {
        cluster.start();

        createAndStartDummyContainer();

        String[] dindImages = {"mesos/logstash-executor"};

        pushDindImagesToPrivateRegistry(clusterConfig.dockerClient, dindImages, clusterConfig);
        pullDindImagesAndRetagWithoutRepoAndLatestTag(clusterConfig.dockerClient,
            cluster.getMesosContainer().getMesosContainerID(), dindImages);


        printRunningContainers();

        Runtime.getRuntime().addShutdownHook(new Thread(cluster::stop));

        System.out.println("Cluster Started.");
        System.out.println("MASTER URL: " + cluster.getMesosContainer().getMesosMasterURL());
        Thread.currentThread().join();
    }

    private String printRunningContainers() {
        DockerClient dockerClient = clusterConfig.dockerClient;
        ExecCreateCmdResponse execCreateCmdResponse;
        InputStream execCmdStream;

        execCreateCmdResponse = dockerClient
            .execCreateCmd(cluster.getMesosContainer().getMesosContainerID())
            .withAttachStdout(true)
            .withCmd("bash", "-c", "docker ps").exec();

        execCmdStream = dockerClient.execStartCmd(execCreateCmdResponse.getId()).exec();
        String runningDockerContainers = DockerUtil.consumeInputStream(execCmdStream);

        System.out.println(runningDockerContainers);

        return runningDockerContainers;
    }

    private void pullDindImagesAndRetagWithoutRepoAndLatestTag(DockerClient dockerClient,
        String mesosClusterContainerId, String[] dindImages) {

        for (String image : dindImages) {

            try {
                Thread.sleep(2000); // we have to wait
            } catch (InterruptedException ignored) {
            }

            ExecCreateCmdResponse execCreateCmdResponse = dockerClient
                .execCreateCmd(mesosClusterContainerId)
                .withAttachStdout(true)
                .withCmd("docker", "pull", "private-registry:5000/" + image + ":systemtest").exec();
            InputStream execCmdStream = dockerClient.execStartCmd(execCreateCmdResponse.getId())
                .exec();
            assertThat(DockerUtil.consumeInputStream(execCmdStream),
                Matchers.containsString("Download complete"));

            execCreateCmdResponse = dockerClient.execCreateCmd(mesosClusterContainerId)
                .withAttachStdout(true)
                .withCmd("docker", "tag", "private-registry:5000/" + image + ":systemtest",
                    image + ":latest").exec();

            execCmdStream = dockerClient.execStartCmd(execCreateCmdResponse.getId()).exec();
            DockerUtil.consumeInputStream(execCmdStream);
        }
    }

    private void pushDindImagesToPrivateRegistry(DockerClient dockerClient,
        String[] dindImages, MesosClusterConfig config) {
        for (String image : dindImages) {
            String imageWithPrivateRepoName =
                "localhost:" + config.privateRegistryPort + "/" + image;
            dockerClient.tagImageCmd(image, imageWithPrivateRepoName, "systemtest").withForce(true)
                .exec();
            InputStream responsePushImage = dockerClient.pushImageCmd(imageWithPrivateRepoName)
                .withTag("systemtest").exec();
            assertThat(DockerUtil.consumeInputStream(responsePushImage),
                Matchers.containsString("The push refers to a repository"));
        }
    }

    private String createAndStartDummyContainer() {

        ExecCreateCmdResponse execCreateCmdResponse = clusterConfig.dockerClient
            .execCreateCmd(cluster.getMesosContainer().getMesosContainerID())
            .withAttachStdout(true)
            .withCmd("docker", "run", "-td", "-v", "/tmp:/tmp/testlogs", "busybox", "sh").exec();

        InputStream execCmdStream = clusterConfig.dockerClient
            .execStartCmd(execCreateCmdResponse.getId()).exec();

        String containerId = DockerUtil.consumeInputStream(execCmdStream)
            .replaceAll("[^a-z0-9]*", "");

        System.out.println(containerId);

        return containerId;
    }

    class LocalMesosCluster extends MesosCluster {

        public LocalMesosCluster(MesosClusterConfig config) {
            super(config);
        }

        public void stop() {
            this.after();
        }
    }

}
