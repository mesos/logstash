package org.apache.mesos.logstash.scheduler;

import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class Application {

    private static final Logger LOGGER = Logger.getLogger(Scheduler.class);


    private static Options OPTIONS = new Options() {{
        addOption("m", "masterURL host or IP", true, "masterURL host or IP");
    }};

    protected Application() {}

    public static void main(String[] args) throws IOException {

        LOGGER.info("Command line arguments: ");

        for (String arg : args) {
            LOGGER.info(arg);
        }

        CommandLine cmdLine = parseCommandLineArgs(args);
        String masterHost = cmdLine.getOptionValue("m", null);

        if (masterHost == null) {
            printUsage(OPTIONS);
            return;
        }

        InputStream is = Application.class.getClassLoader().getResourceAsStream("executor-name.txt");
        String executorImageName = IOUtils.toString(is);

        LOGGER.info("Executor Name: " + executorImageName);

        final Scheduler scheduler = new Scheduler(masterHost, executorImageName);

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                scheduler.stop();
            }
        }));

        LOGGER.info("Starting Logstash on Mesos");

        scheduler.run();
    }

    private static void printUsage(Options options) {
        LOGGER.info("User doesn't know what he is doing");

        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(Configuration.FRAMEWORK_NAME, options);
    }

    private static CommandLine parseCommandLineArgs(String[] args) {
        try {
            CommandLineParser parser = new BasicParser();
            return parser.parse(OPTIONS, args);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
