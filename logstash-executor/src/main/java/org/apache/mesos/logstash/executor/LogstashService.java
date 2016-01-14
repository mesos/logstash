package org.apache.mesos.logstash.executor;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.mesos.logstash.common.LogstashProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Encapsulates a logstash instance. Keeps track of the current container id for logstash.
 */
public class LogstashService {

    public static final Logger LOGGER = LoggerFactory.getLogger(LogstashService.class);

    private static String serialize(LogstashProtos.LogstashConfiguration logstashConfiguration) {
        List<LS.Plugin> inputPlugins = optionalValuesToList(
                Optional.ofNullable(logstashConfiguration.getLogstashPluginInputSyslog()).map(config -> LS.plugin("syslog", LS.map(LS.kv("port", LS.number(config.getPort()))))),
                Optional.ofNullable(logstashConfiguration.getLogstashPluginInputCollectd()).map(config -> LS.plugin("udp", LS.map(LS.kv("port", LS.number(5000 /*TODO: config.getPort()*/)), LS.kv("buffer_size", LS.number(1452)), LS.kv("codec", LS.plugin("collectd", LS.map()))))),
                Optional.ofNullable(logstashConfiguration.getLogstashPluginInputFile()).map(config -> LS.plugin("file", LS.map(LS.kv("path", LS.array(config.getPathList().stream().map(path -> "/logstashpaths" + path).map(LS::string).toArray(LS.Value[]::new))))))
        );

        List<LS.Plugin> outputPlugins = optionalValuesToList(
                Optional.ofNullable(logstashConfiguration.getLogstashPluginOutputElasticsearch()).map(config -> LS.plugin(
                        "elasticsearch",
                        LS.map(
                                LS.kv("host", LS.string(config.getHost())),
                                LS.kv("protocol", LS.string("http")),
                                LS.kv("index", LS.string("logstash"))  //FIXME this should be configurable. Maybe add -%{+YYYY.MM.dd}
                        )
                ))
        );


        return LS.config(
                LS.section("input",  inputPlugins.toArray(new LS.Plugin[inputPlugins.size()])),
                LS.section("output", outputPlugins.toArray(new LS.Plugin[outputPlugins.size()]))
        ).serialize();
    }

    @SafeVarargs
    private static <T> List<T> optionalValuesToList(Optional<T> ... optionals) {
        return Arrays.stream(optionals).filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());
    }

    public void run(LogstashProtos.LogstashConfiguration logstashConfiguration) {
        LOGGER.info("Starting the Logstash Process.");

        Process process;
        try {
            String[] command = {
                    "/opt/logstash/bin/logstash",
                    "--log", "/var/log/logstash.log",
                    "-e", serialize(logstashConfiguration)
            };
            String[] env = {
                    "LS_HEAP_SIZE=" + System.getProperty("mesos.logstash.logstash.heap.size"),
                    "HOME=/root"
            };
            LOGGER.info("Starting subprocess: " + String.join(" ", env) + " " + String.join(" ", command));
            process = Runtime.getRuntime().exec(command, env);
        } catch (IOException e) {
            throw new RuntimeException("Failed to start Logstash", e);
        }

        try {
            inputStreamForEach((s) -> LOGGER.info("Logstash stdout: " + s), process.getInputStream());
            inputStreamForEach((s) -> LOGGER.warn("Logstash stderr: " + s), process.getErrorStream());

            process.waitFor();
            LOGGER.warn("Logstash quit with exit={}", process.exitValue());
        } catch (InterruptedException e) {
            throw new RuntimeException("Logstash process was interrupted", e);
        }

        try {
            IOUtils.copy(process.getErrorStream(), System.err);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read STDERR of Logstash");
        }
    }

    private static void inputStreamForEach(Consumer<String> consumer, InputStream inputStream) {
        new Thread(() -> new BufferedReader(new InputStreamReader(inputStream)).lines().forEach(consumer)).start();
    }
}
