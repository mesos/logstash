package org.apache.mesos.logstash.executor;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.mesos.logstash.common.LogstashProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Encapsulates a logstash instance. Keeps track of the current container id for logstash.
 */
public class LogstashService {

    public static final Logger LOGGER = LoggerFactory.getLogger(LogstashService.class);

    public static final String LOGSTASH_PATH = System.getenv("LOGSTASH_PATH");

    private static <T> Optional<T> ofConditional(T message, Predicate<T> predicate) {
        if (message != null && predicate.test(message)) {
            return Optional.of(message);
        }
        return Optional.empty();
    }

    private static String serialize(LogstashProtos.LogstashConfiguration logstashConfiguration) {
        LOGGER.info("Received " + logstashConfiguration.toString());
        LOGGER.info("logstashConfiguration.getLogstashPluginOutputElasticsearch() = " + logstashConfiguration.getLogstashPluginOutputElasticsearch().isInitialized());


        List<LS.Plugin> inputPlugins = optionalValuesToList(
                ofConditional(logstashConfiguration.getLogstashPluginInputSyslog(), LogstashProtos.LogstashPluginInputSyslog::isInitialized).map(config -> LS.plugin("syslog", LS.map(LS.kv("port", LS.number(config.getPort()))))),
                ofConditional(logstashConfiguration.getLogstashPluginInputCollectd(), LogstashProtos.LogstashPluginInputCollectd::isInitialized).map(config -> LS.plugin("udp", LS.map(LS.kv("port", LS.number(5000 /*TODO: config.getPort()*/)), LS.kv("buffer_size", LS.number(1452)), LS.kv("codec", LS.plugin("collectd", LS.map()))))),
                ofConditional(logstashConfiguration.getLogstashPluginInputFile(), LogstashProtos.LogstashPluginInputFile::isInitialized).map(config -> LS.plugin("file", LS.map(LS.kv("path", LS.array(config.getPathList().stream().map(path -> "/logstashpaths" + path).map(LS::string).toArray(LS.Value[]::new))))))
        );

        List<LS.Plugin> filterPlugins = Arrays.asList(
            LS.plugin("mutate", LS.map(
                    LS.kv("add_field", LS.map(
                            LS.kv("mesos_slave_id", LS.string(logstashConfiguration.getMesosSlaveId()))
                        )
                    )
                )
            )
        );

        List<LS.Plugin> outputPlugins = optionalValuesToList(
                ofConditional(logstashConfiguration.getLogstashPluginOutputElasticsearch(), LogstashProtos.LogstashPluginOutputElasticsearch::isInitialized).map(config -> {
                    URL url;
                    try {
                        url = new URL(config.getUrl());
                    } catch (MalformedURLException e) {
                        throw new RuntimeException("Failed to parse Elasticsearch URL: " + config.getUrl(), e);
                    }
                    return LS.plugin(
                            "elasticsearch",
                            LS.map(
                                    filterEmpties(
                                            LS.KV.class,
                                            Optional.of(LS.kv("host", LS.string(url.getHost()))),
                                            Optional.of(LS.kv("port", LS.number(url.getPort() > 0 ? url.getPort() : 9200))),
                                            Optional.of(LS.kv("protocol", LS.string(url.getProtocol()))),
                                            ofConditional(config.getIndex(), StringUtils::isNotEmpty).map(index -> LS.kv("index", LS.string(index)))
                                    )
                            )
                    );
                })
        );

        return LS.config(
                LS.section("input",  inputPlugins.toArray(new LS.Plugin[inputPlugins.size()])),
                LS.section("filter", filterPlugins.toArray(new LS.Plugin[filterPlugins.size()])),
                LS.section("output", outputPlugins.toArray(new LS.Plugin[outputPlugins.size()]))
        ).serialize();
    }

    private static <T> T[] filterEmpties(Class<T> type, Optional<T>... optionals) {
        return Arrays.stream(optionals).filter(Optional::isPresent).map(Optional::get).toArray(size -> (T[]) Array.newInstance(type, size));
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
                    LOGSTASH_PATH,
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
