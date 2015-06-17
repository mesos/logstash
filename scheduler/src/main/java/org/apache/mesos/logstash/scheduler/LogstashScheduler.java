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

    private MesosSchedulerDriver driver;

    // As per the DCOS Service Specification, setting the failover timeout to a large value;
    private static final double FAILOVER_TIMEOUT = 86400000;
    private Protos.FrameworkID frameworkId;

    public LogstashScheduler(String master, String configFilePath) {
        this.master = master;
        this.configFilePath = configFilePath;
        this.driver = buildSchedulerDriver();


    }

    public static final Options OPTIONS = new Options();


    static {
        OPTIONS.addOption("f", "logstash config file", true, "logstash config file");
        OPTIONS.addOption("m", "master host or IP", true, "master host or IP");
    }

    public static void main(String[] args) {
        logArgs(args);

        String masterHost, configPath;
        CommandLine cmdLine = parseCommandLineArgs(args);
        try {
            masterHost = cmdLine.getOptionValue("m");
            configPath = cmdLine.getOptionValue("f");
        }
        catch(NullPointerException e) {
            printUsage(OPTIONS);
            return;
        }

        if(masterHost == null || configPath == null) {
            printUsage(OPTIONS);
            return;
        }

        startLogstash(masterHost, configPath);
    }

    private static void logArgs(String[] args) {
        LOGGER.info("Command line arguments: ");
        for(int i = 0; i < args.length; ++i) {
            LOGGER.info(args[i]);
        }
    }

    private static CommandLine parseCommandLineArgs(String[] args) {
        CommandLineParser parser = new BasicParser();

        try {
            return parser.parse(OPTIONS, args);
        }
        catch (ParseException e) {
            e.printStackTrace(System.err);
            return null;
        }
    }

    private static void startLogstash(String masterHost, String configPath) {
        LOGGER.info("Starting Logstash on Mesos");
        LOGGER.info("Config path: " + configPath);

        final LogstashScheduler scheduler = new LogstashScheduler(masterHost, configPath);
        scheduler.installShutdownHook();

        LOGGER.info("Starting scheduler..");
        new Thread(scheduler).start();
    }

    private void installShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                LogstashScheduler.this.getDriver().stop();
                LogstashScheduler.this.onShutdown();
            }
        }));
    }

    public MesosSchedulerDriver getDriver() {
        return driver;
    }

    private MesosSchedulerDriver buildSchedulerDriver() {
        final Protos.FrameworkInfo frameworkInfo = buildFramework();

        LOGGER.info("Connecting to master " + getMesosUrl());

        return new MesosSchedulerDriver(this, frameworkInfo, getMesosUrl());
    }

    private Protos.FrameworkInfo buildFramework() {
        final Protos.FrameworkInfo.Builder frameworkBuilder = Protos.FrameworkInfo.newBuilder();
        frameworkBuilder.setName(FRAMEWORK_NAME);
        frameworkBuilder.setUser("root"); // TODO change
        frameworkBuilder.setCheckpoint(true);
        frameworkBuilder.setFailoverTimeout(FAILOVER_TIMEOUT);
        return frameworkBuilder.build();
    }

    private void onShutdown() {
        LOGGER.info("On shutdown...");
    }

    private static void printUsage(Options options) {
        LOGGER.info("User doesn't know what he is doing");

        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(FRAMEWORK_NAME, options);
    }

    public String getMesosUrl() {
        return this.master + ":" + MESOS_PORT;
    }

    @Override
    public void run() {
        LOGGER.info("Starting up ...");
        driver.run();

    }

    @Override
    public void registered(SchedulerDriver schedulerDriver, Protos.FrameworkID frameworkID, Protos.MasterInfo masterInfo) {
        LOGGER.info("Registered against Mesos");
        this.frameworkId = frameworkID;
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
        LOGGER.info("Offer rescinded");

    }

    @Override
    public void statusUpdate(SchedulerDriver schedulerDriver, Protos.TaskStatus taskStatus) {
        LOGGER.info("Task status update! " + taskStatus.toString());
    }

    @Override
    public void frameworkMessage(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, byte[] bytes) {
        LOGGER.info("Message received");

    }

    @Override
    public void disconnected(SchedulerDriver schedulerDriver) {
        LOGGER.error("Disconnected :(");
    }

    @Override
    public void slaveLost(SchedulerDriver schedulerDriver, Protos.SlaveID slaveID) {
        LOGGER.error("Slave losta");
    }

    @Override
    public void executorLost(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, int i) {
        LOGGER.error("Executor losta");
    }

    @Override
    public void error(SchedulerDriver schedulerDriver, String s) {
        LOGGER.error("It broke.");
    }

    private String taskId(Protos.Offer offer) {
        String date = new SimpleDateFormat(TASK_DATE_FORMAT).format(clock.now());
        return String.format(FRAMEWORK_NAME + "_%s_%s", offer.getHostname(), date);
    }


    private Protos.TaskInfo buildTask(SchedulerDriver driver, Protos.Offer offer, String id) {

        LOGGER.info("BUILDING task!");
        Protos.TaskInfo.Builder taskInfoBuilder = Protos.TaskInfo.newBuilder()
                .setName(Configuration.TASK_NAME)
                .setTaskId(Protos.TaskID.newBuilder().setValue(id))
                .setSlaveId(offer.getSlaveId())
                .addAllResources(offer.getResourcesList());

        Protos.ContainerInfo.DockerInfo.Builder dockerExecutor = Protos.ContainerInfo.DockerInfo.newBuilder()
                .setForcePullImage(true)
                .setImage("epeld/logstash-executor");


            LOGGER.info("Using Executor to start Logstash cloud mesos on slaves");
            Protos.ExecutorInfo executorInfo = Protos.ExecutorInfo.newBuilder()
                    .setExecutorId(Protos.ExecutorID.newBuilder().setValue(UUID.randomUUID().toString()))
                    .setFrameworkId(frameworkId)
                    .setContainer(Protos.ContainerInfo.newBuilder().setType(Protos.ContainerInfo.Type.DOCKER).setDocker(dockerExecutor.build()))
                    .setName("" + UUID.randomUUID())
                    .setCommand(Protos.CommandInfo.newBuilder()
                            .addArguments("java")
                            .addArguments("-Djava.library.path=/usr/local/lib")
                            .addArguments("-jar")
                            .addArguments("/tmp/logstash-executor.jar")
                            .setShell(false))
                    .build();

            taskInfoBuilder.setExecutor(executorInfo);

        return taskInfoBuilder.build();
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
