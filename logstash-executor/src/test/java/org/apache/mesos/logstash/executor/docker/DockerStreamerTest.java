package org.apache.mesos.logstash.executor.docker;

import com.spotify.docker.client.DockerCertificateException;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.messages.ContainerCreation;
import org.apache.mesos.logstash.executor.TestableLogStream;
import org.apache.mesos.logstash.executor.logging.LogStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DockerStreamerTest {

    public static final String SOME_CONTAINER_ID = "123";
    public static final String SOME_LOG_LOCATION = "some/log/location";
    private DockerStreamer streamer;
    private ByteBufferLogSteamWriter writer;
    private DockerClient dockerClient;
    private ContainerCreation container;
    private com.spotify.docker.client.DockerClient client;
    private TestableLogStream testLogStream;

    @Before
    public void setUp() throws com.spotify.docker.client.DockerException, InterruptedException,
        DockerCertificateException {

        dockerClient = mock(DockerClient.class);

        writer = new ByteBufferLogSteamWriter();

        streamer = new DockerStreamer(writer, dockerClient);

        testLogStream = new TestableLogStream();

        when(dockerClient
            .exec(SOME_CONTAINER_ID, "sh", "-c", streamer.getMonitorCmd(
                SOME_LOG_LOCATION))).thenReturn(
            testLogStream);
    }

    @After
    public void tearDown() {

    }

    @Test
    public void filesAreStreamedToTheExecutor()
        throws DockerException, InterruptedException, IOException {

        LogStream logStream = null;

        try {

            logStream = streamer
                .startStreaming(new DockerLogPath(SOME_CONTAINER_ID, "busybox", SOME_LOG_LOCATION));

            testLogStream.outputStream.write("Hello\n".getBytes("UTF-8"));

            assertEquals(testLogStream, logStream);
            assertEquals("Hello\n", writer.getStdOutContent());
        } finally {
            if (logStream != null)
                logStream.close();
        }
    }

}
