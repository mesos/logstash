package org.apache.mesos.logstash.executor;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.logstash.LogstashConnector;
import org.apache.mesos.logstash.LogstashService;
import org.apache.mesos.logstash.docker.DockerInfoImpl;
import org.apache.mesos.logstash.logging.LogfileStreaming;

import java.util.Enumeration;
import java.util.logging.Logger;
import java.net.*;

import static java.util.concurrent.TimeUnit.HOURS;

/**
 * Created by ero on 03/07/15.
 */
public class Application {
    static final Logger LOGGER = Logger.getLogger(Application.class.toString());

    public static void main(String[] args) {
        LOGGER.info("Application running!");

        runExecutor(createLogstashConnector());
    }

    private static void runExecutor(LogstashConnector connector) {
        MesosExecutorDriver driver = new MesosExecutorDriver(new Executor(connector));

        Protos.Status status = driver.run();
        if (status.equals(Protos.Status.DRIVER_STOPPED)) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }


    private static LogstashConnector createLogstashConnector() {
        DockerClient dockerClient = createDockerClient();
        DockerInfoImpl dockerInfo = new DockerInfoImpl(dockerClient);
        LogstashService service = new LogstashService();
        LogfileStreaming streaming = new LogfileStreaming(dockerInfo);
        return new LogstashConnector(dockerInfo, service, streaming);
    }

    private static com.spotify.docker.client.DockerClient createDockerClient() {
        return DefaultDockerClient.builder()
                .readTimeoutMillis(HOURS.toMillis(1))
                .uri(URI.create(getHostAddress()))
                .build();
    }

    private static String getHostAddress() {
        String hostAddress = null;
        try {
            Enumeration<InetAddress> inetAddresses = NetworkInterface.getByName("eth0").getInetAddresses();
            while (inetAddresses.hasMoreElements()) {
                InetAddress a = inetAddresses.nextElement();
                if (a instanceof Inet6Address) {
                    continue;
                }

                hostAddress = String.format("http:/%s:2376", a.toString());
                LOGGER.info("Host address is: " + hostAddress);
            }
        } catch (SocketException se) {
            se.printStackTrace();
        }

        return hostAddress;
    }
}
