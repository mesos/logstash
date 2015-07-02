package org.apache.mesos.logstash.executor;

import com.google.protobuf.InvalidProtocolBufferException;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import freemarker.cache.TemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import org.apache.log4j.Logger;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.logstash.common.LogstashProtos;

import static java.util.concurrent.TimeUnit.HOURS;
import static org.apache.mesos.logstash.common.LogstashProtos.*;

import java.io.File;
import java.io.IOException;
import java.net.*;
import java.util.*;

/**
 * Executor for Logstash.
 */
@SuppressWarnings("PMD.TooManyMethods")
public class LogstashExecutor implements Executor {

    public static final Logger LOGGER = Logger.getLogger(LogstashExecutor.class.toString());

    private LogstashConnector logstashConnector = null;

    public static void main(String[] args) {

        LOGGER.info("Executor running?!");

        LOGGER.info("Started LogstashExecutor");

        MesosExecutorDriver driver = new MesosExecutorDriver(new LogstashExecutor());
        Protos.Status status = driver.run();
        if (status.equals(Protos.Status.DRIVER_STOPPED)) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }

    @Override
    public void registered(ExecutorDriver driver, Protos.ExecutorInfo executorInfo, Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {
        LOGGER.info("Executor Logstash registered on slave " + slaveInfo.getHostname());
    }

    @Override
    public void reregistered(ExecutorDriver driver, Protos.SlaveInfo slaveInfo) {
        LOGGER.info("Executor Logstash re-registered on slave " + slaveInfo.getHostname());
    }

    @Override
    public void disconnected(ExecutorDriver driver) {
        LOGGER.info("Executor Logstash disconnected");
    }

    @Override
    public void launchTask(final ExecutorDriver driver, final Protos.TaskInfo task) {
        Protos.TaskStatus status = Protos.TaskStatus.newBuilder()
                .setTaskId(task.getTaskId())
                .setState(Protos.TaskState.TASK_RUNNING).build();
        driver.sendStatusUpdate(status);

        String hostAddress = getHostAddress();

        doIt(driver, hostAddress);

        try {
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    Protos.TaskStatus taskStatus = Protos.TaskStatus.newBuilder()
                            .setTaskId(task.getTaskId())
                            .setState(Protos.TaskState.TASK_FINISHED).build();
                    driver.sendStatusUpdate(taskStatus);
                }
            }) {
            });
        } catch (Exception e) {
            status = Protos.TaskStatus.newBuilder()
                    .setTaskId(task.getTaskId())
                    .setState(Protos.TaskState.TASK_FAILED).build();
            driver.sendStatusUpdate(status);
        }
    }

    private void doIt(final ExecutorDriver executorDriver, String hostAddress) {
        LOGGER.info("Host address is: " + hostAddress);

        DockerClient dockerClient = DefaultDockerClient.builder()
                .readTimeoutMillis(HOURS.toMillis(1))
                .uri(URI.create(hostAddress))
                .build();

        FrameworkDiscoveryListener frameworkDiscoveryListener = new FrameworkDiscoveryListener() {
            @Override
            public void frameworksDiscovered(List<String> frameworkNames) {
                LOGGER.info(String.format("Sending framework message..."));

                for (String frameworkName : frameworkNames) {
                    LOGGER.info(String.format("Framework name: %s", frameworkName));
                }

                Protos.Status status = executorDriver.sendFrameworkMessage(createExecutorMessage(frameworkNames));

                LOGGER.info(String.format("Driver status %s", status));
            }
        };

        try {
            createLogstashConnector(dockerClient, frameworkDiscoveryListener);
        }
        catch(IOException e) {
            LOGGER.error(String.format("Couldn't load config template %s", e));
        }
    }

    private void createLogstashConnector(DockerClient dockerClient,
                                                      FrameworkDiscoveryListener frameworkDiscoveryListener) throws IOException {
        if(this.logstashConnector == null) {
            Template configTemplate = initTemplatingEngine().getTemplate("conf.ftl");

            LOGGER.info("Config template loaded");

            LogstashService logstash = new LogstashService(configTemplate);

            LOGGER.info("logstash service created");

            DockerInfo dockerInfo = new DockerInfoImpl(dockerClient, frameworkDiscoveryListener);
            this.logstashConnector = new LogstashConnector(dockerInfo, logstash, new LogfileStreaming(dockerInfo));

            LOGGER.info("connector set up");
        }
    }

    private byte[] createExecutorMessage(List<String> frameworkNames) {
        return ExecutorMessage.newBuilder()
                .addAllFrameworkName(frameworkNames)
                .build()
                .toByteArray();
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

    @Override
    public void killTask(ExecutorDriver driver, Protos.TaskID taskId) {
        LOGGER.info("Kill task: " + taskId.getValue());
        Protos.TaskStatus status = Protos.TaskStatus.newBuilder()
                .setTaskId(taskId)
                .setState(Protos.TaskState.TASK_FAILED).build();
        driver.sendStatusUpdate(status);
    }

    @Override
    public void frameworkMessage(ExecutorDriver driver, byte[] data) {
        LOGGER.info("Framework message: " + Arrays.toString(data));

        try {
            SchedulerMessage schedulerMessage = SchedulerMessage.parseFrom(data);

            Map<String, String> dockerConfigs = extractConfigs(schedulerMessage.getDockerConfigList());
            Map<String, String> hostConfigs = extractConfigs(schedulerMessage.getHostConfigList());

            if(this.logstashConnector != null) {
                // TODO fixme when we have determined logic flow to logstash
                // this.logstashConnector.updatedLogLocations(parseFrameworks(schedulerMessage));
            }

        } catch (InvalidProtocolBufferException e) {
            LOGGER.error("Error parsing framework message from scheduler", e);
        } catch (Exception e) {
            LOGGER.error("Unexpected error", e);
        }
    }

    private Map<String,String> extractConfigs(List<LogstashConfig> cfgs) {
        Map<String, String> configs = new HashMap<>();
        cfgs.stream()
                .forEach(cfg ->
                        configs.put(cfg.getFrameworkName(), cfg.getConfig()));
        return configs;
    }


    @Override
    public void shutdown(ExecutorDriver driver) {
        LOGGER.info("Shutting down framework...");
    }

    @Override
    public void error(ExecutorDriver driver, String message) {
        LOGGER.info("Error in executor: " + message);
    }

    private Configuration initTemplatingEngine() {
        Configuration conf = new Configuration();
        conf.setDefaultEncoding("UTF-8");

        conf.setClassForTemplateLoading(this.getClass(), "/");
        conf.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

        return conf;
    }
}
