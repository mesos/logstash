package org.apache.mesos.logstash.executor.docker;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerCertificateException;
import com.spotify.docker.client.DockerCertificates;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import org.apache.mesos.logstash.executor.logging.ByteBufferLogSteamWriter;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

import static com.jayway.awaitility.Awaitility.await;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.junit.Assert.assertThat;

@Ignore
public class DockerStreamerTest {

    private DockerStreamer streamer;
    private ByteBufferLogSteamWriter writer;
    private DockerClient dockerClient;
    private ContainerCreation container;
    private com.spotify.docker.client.DockerClient client;

    @Before
    public void setUp() throws com.spotify.docker.client.DockerException, InterruptedException,
        DockerCertificateException {

        writer = new ByteBufferLogSteamWriter();

        DockerCertificates certs = DockerCertificates.builder()
            .dockerCertPath(Paths.get(System.getenv("DOCKER_CERT_PATH"))).build();

        client = DefaultDockerClient.builder()
            .readTimeoutMillis(HOURS.toMillis(1))
            .uri(URI.create(System.getenv("DOCKER_HOST").replace("tcp", "https")))
            .dockerCertificates(certs)
            .build();

        client.pull("busybox");

        dockerClient = new org.apache.mesos.logstash.executor.docker.DockerClient(client);

        streamer = new DockerStreamer(writer, dockerClient);
    }

    @After
    public void tearDown() {
        try {
            client.stopContainer(container.id(), 5);
        } catch (Exception e) {
            System.err.println("Failed to stop container");
        }
    }

    @Test
    public void filesAreStreamedToTheExecutor() throws DockerException, InterruptedException {

        container = client.createContainer(ContainerConfig.builder()
            .image("busybox") // while : sleep 3; echo 1; done
            .cmd("sh", "-c", "echo 'Hello' >> /mytest.log && sleep 10").build());

        client.startContainer(container.id());

        streamer.startStreaming(new DockerLogPath(container.id(), "busybox", "/mytest.log"));

        await().until(() -> {
            return writer.getStdOutContent().equals("Hello\n");
        });
    }

    @Test
    public void filesAreStreamedToTheExecutor2()
        throws DockerException, InterruptedException, UnsupportedEncodingException {

        container = client.createContainer(ContainerConfig.builder()
            .image("busybox")
            .cmd("sh", "-c", "while sleep 1; do echo 'foo' >> /mytest.log; done").build());

        client.startContainer(container.id());

        streamer.startStreaming(new DockerLogPath(container.id(), "busybox", "/mytest.log"));

        final AtomicInteger count = new AtomicInteger(0);

        await().until(() -> {
            int c = writer.getStdOutContent().split("\n").length;
            count.set(c);
            return c > 1;
        });

        Thread.sleep(3_000);

        int messageCount = writer.getStdOutContent().split("\n").length;
        assertThat(messageCount, Matchers.greaterThanOrEqualTo(count.get() + 3));
    }
}
