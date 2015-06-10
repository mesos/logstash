package org.apache.mesos.logstash.scheduler;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ContainerInfo.DockerInfo.PortMapping;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by ero on 08/06/15.
 */
public class LogstashScheduler implements Scheduler, Runnable {

    public static final double RESOURCE_FACTOR = 0.1;

    public static final Logger LOGGER = Logger.getLogger(LogstashScheduler.class.toString());
    private static final int MESOS_PORT = 5050;
    private static final String FRAMEWORK_NAME = "logstash";
    public static final String TASK_DATE_FORMAT = "yyyyMMdd'T'HHmmss.SSS'Z'";

    private Clock clock = new Clock();
    private Set<Task> tasks = new HashSet<>();
    private String master; // the URL of the mesos master
    private String configFilePath;

    // As per the DCOS Service Specification, setting the failover timeout to a large value;
    private static final double FAILOVER_TIMEOUT = 86400000;

    public LogstashScheduler(String master, String configFilePath) {
        this.master = master;
        this.configFilePath = configFilePath;
    }

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("f", "logstash config file", true, "logstash config file");
        options.addOption("m", "master host or IP", true, "master host or IP");

        CommandLineParser parser = new BasicParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            String masterHost = cmd.getOptionValue("m");
            String configPath = cmd.getOptionValue("f");

            if (masterHost == null) {
                printUsage(options);
                return;
            }

            LOGGER.info("Starting Logstash on Mesos");
            LOGGER.info("Config path: " + configPath);

            final LogstashScheduler scheduler = new LogstashScheduler(masterHost, configPath);

            final MesosSchedulerDriver driver = scheduler.buildSchedulerDriver();

            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    driver.stop();
                    scheduler.onShutdown();
                }
            }));

            new Thread(scheduler).start();
        } catch (ParseException e) {
            printUsage(options);
        }
    }

    private MesosSchedulerDriver buildSchedulerDriver() {
        final Protos.FrameworkInfo frameworkInfo = buildFramework();

        return new MesosSchedulerDriver(this, frameworkInfo, this.master + ":" + MESOS_PORT);
    }

    private Protos.FrameworkInfo buildFramework() {
        final Protos.FrameworkInfo.Builder frameworkBuilder = Protos.FrameworkInfo.newBuilder();
        frameworkBuilder.setName(FRAMEWORK_NAME);
        frameworkBuilder.setUser("triforkse");
        frameworkBuilder.setCheckpoint(true);
        frameworkBuilder.setFailoverTimeout(FAILOVER_TIMEOUT);
        return frameworkBuilder.build();
    }

    private void onShutdown() {
        LOGGER.info("On shutdown...");
    }

    private static void printUsage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(FRAMEWORK_NAME, options);
    }

    @Override
    public void run() {
        LOGGER.info("Starting up ...");
        SchedulerDriver driver = new MesosSchedulerDriver(this, Protos.FrameworkInfo.newBuilder().setUser("").setName(FRAMEWORK_NAME).build(), master + ":" + MESOS_PORT);
        driver.run();

    }

    @Override
    public void registered(SchedulerDriver schedulerDriver, Protos.FrameworkID frameworkID, Protos.MasterInfo masterInfo) {
        LOGGER.info("Registered against Mesos");
    }

    @Override
    public void reregistered(SchedulerDriver schedulerDriver, Protos.MasterInfo masterInfo) {
        LOGGER.info("Reregistered against Mesos");
    }

    @Override
    public void resourceOffers(SchedulerDriver schedulerDriver, List<Protos.Offer> list) {
        for (Protos.Offer offer : list) {
            LOGGER.info("Received offer from slave " + offer.getSlaveId() + ", " + offer.getHostname());

            if (shouldAcceptOffer(offer)) {
                LOGGER.info("Offer accepted");

                String id = taskId(offer);
                Protos.TaskInfo taskInfo = buildTask(schedulerDriver, offer, id);

                schedulerDriver.launchTasks(Collections.singleton(offer.getId()), Collections.singleton(taskInfo));
                tasks.add(new Task(offer.getHostname(), id));
            }
            else {
                LOGGER.info("Offer declined");
                schedulerDriver.declineOffer(offer.getId());
            }
        }
    }

    @Override
    public void offerRescinded(SchedulerDriver schedulerDriver, Protos.OfferID offerID) {

    }

    @Override
    public void statusUpdate(SchedulerDriver schedulerDriver, Protos.TaskStatus taskStatus) {
        LOGGER.info("Task status update! " + taskStatus.toString());
    }

    @Override
    public void frameworkMessage(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, byte[] bytes) {

    }

    @Override
    public void disconnected(SchedulerDriver schedulerDriver) {

    }

    @Override
    public void slaveLost(SchedulerDriver schedulerDriver, Protos.SlaveID slaveID) {

    }

    @Override
    public void executorLost(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, int i) {

    }

    @Override
    public void error(SchedulerDriver schedulerDriver, String s) {

    }

    private String taskId(Protos.Offer offer) {
        String date = new SimpleDateFormat(TASK_DATE_FORMAT).format(clock.now());
        return String.format(FRAMEWORK_NAME + "_%s_%s", offer.getHostname(), date);
    }

    private Integer selectFirstPort(List<Protos.Resource> offeredResources) {
        for (Protos.Resource resource : offeredResources) {
            if (resource.getType().equals(Protos.Value.Type.RANGES)) {
                return Integer.valueOf((int) resource.getRanges().getRangeList().get(0).getBegin());
            }
        }
        return null;
    }

    private void addAllScalarResources(List<Protos.Resource> offeredResources, List<Protos.Resource> acceptedResources) {


        for (Protos.Resource resource : offeredResources) {
            if (resource.getType().equals(Protos.Value.Type.SCALAR)) {

                Protos.Value.Scalar sc = resource.getScalar();
                double value = sc.getValue() * RESOURCE_FACTOR;

                if(resource.getName().equals("cpus")) {
                    value = 1;
                }

                Protos.Resource rsc = Protos.Resource.newBuilder()
                        .setName(resource.getName())
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder().setValue(value).build())
                        .build();

                acceptedResources.add(rsc);
            }
        }
    }

    private Protos.TaskInfo buildTask(SchedulerDriver driver, Protos.Offer offer, String id) {


        List<Protos.Resource> acceptedResources = new ArrayList<>();

        addAllScalarResources(offer.getResourcesList(), acceptedResources);

        Integer port = selectFirstPort(offer.getResourcesList());

        if (port == null) {
            LOGGER.info("Declined offer: Offer did not contain 1 port");
            driver.declineOffer(offer.getId());
        } else {
            LOGGER.info("Logstash transport port " + port);
            acceptedResources.add(Resources.singlePortRange(port));
        }

        Protos.TaskInfo.Builder taskInfoBuilder = Protos.TaskInfo.newBuilder()
                .setName(Configuration.TASK_NAME)
                .setTaskId(Protos.TaskID.newBuilder().setValue(id))
                .setSlaveId(offer.getSlaveId())
                .addAllResources(acceptedResources);


        LOGGER.info("Using Docker to start Logstash cloud mesos on slaves");
        Protos.ContainerInfo.Builder containerInfo = Protos.ContainerInfo.newBuilder();
        PortMapping transportPortMapping = PortMapping.newBuilder().setContainerPort(Configuration.LOGSTASH_TRANSPORT_PORT).setHostPort(port).build();


        Protos.ContainerInfo.DockerInfo.Builder docker = Protos.ContainerInfo.DockerInfo.newBuilder()
                .setNetwork(Protos.ContainerInfo.DockerInfo.Network.BRIDGE)
                .setImage("library/logstash")
                .addPortMappings(transportPortMapping);



        /*
        String configContent = ("input {\n" +
                "    stdin {\n" +
                "    }\n" +
                "\n" +
                "}\n" +
                "output {\n" +
                "    stdout { }\n" +
                "}").replace("\n", " ");
                */
        String configContent = readFileAsString(configFilePath);
        LOGGER.info("CONFIG: "+ configContent);


        Protos.CommandInfo commandInfo = Protos.CommandInfo.newBuilder()
                .addArguments("logstash")
                .addArguments("-e")
                .addArguments(configContent)
                .setShell(false).build();

        LOGGER.info(commandInfo.toString());

        containerInfo.setDocker(docker.build());
        containerInfo.setType(Protos.ContainerInfo.Type.DOCKER);
        taskInfoBuilder.setContainer(containerInfo);
        taskInfoBuilder
                .setCommand(commandInfo);

        LOGGER.info("Using Docker to start logstash cloud mesos on slaves");
        return taskInfoBuilder.build();
    }

    public static String readFileAsString(String filePath) {
        try(FileInputStream inputStream = new FileInputStream(filePath)) {
            return IOUtils.toString(inputStream);
        }
        catch(IOException e) {
            LOGGER.error(e);
            return "";
        }
    }

    private boolean shouldAcceptOffer(Protos.Offer offer) {
        // Don't start the same framework multiple times on the same host
        for (Task task : tasks) {
            if (task.getHostname().equals(offer.getHostname())) {
                return false;
            }
        }
        return true;
    }
}
