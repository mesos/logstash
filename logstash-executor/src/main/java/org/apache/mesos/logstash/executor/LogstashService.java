package org.apache.mesos.logstash.executor;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.mesos.logstash.common.ExecutorBootConfiguration;
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
import java.util.HashMap;
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

    private static String serialize(ExecutorBootConfiguration bootConfiguration) {
        LOGGER.info("Received " + bootConfiguration.toString());


        List<LS.Plugin> inputPlugins = optionalValuesToList(
                ofConditional(bootConfiguration, ExecutorBootConfiguration::isEnableSyslog).map(config -> LS.plugin("syslog", LS.map(LS.kv("port", LS.number(config.getSyslogPort()))))),
                ofConditional(bootConfiguration, ExecutorBootConfiguration::isEnableCollectd).map(config -> LS.plugin("udp", LS.map(LS.kv("port", LS.number(config.getCollectdPort())), LS.kv("buffer_size", LS.number(1452)), LS.kv("codec", LS.plugin("collectd", LS.map()))))),
                ofConditional(bootConfiguration, ExecutorBootConfiguration::isEnableFile).map(config -> LS.plugin("file", LS.map(LS.kv("path", LS.array(Arrays.stream(config.getFilePaths()).map(path -> (isRunningInDocker() ? "/logstashpaths" : "") + path).map(LS::string).toArray(LS.Value[]::new))))))
        );

        List<LS.Plugin> filterPlugins = Arrays.asList(
                LS.plugin("mutate", LS.map(
                        LS.kv("add_field", LS.map(
                                LS.kv("mesos_slave_id", LS.string(bootConfiguration.getMesosAgentId()))
                                )
                        )
                        )
                )
        );

        List<LS.Plugin> outputPlugins = optionalValuesToList(
                Optional.ofNullable(bootConfiguration.getElasticSearchUrl()).map(LogstashService::parseURL).map(url -> LS.plugin(
                        "elasticsearch",
                        LS.map(
                                filterEmpties(
                                        LS.KV.class,
                                        Optional.of(LS.kv("hosts", LS.string(url.getHost() + ":" + (url.getPort() > 0 ? url.getPort() : 9200)))),
                                        Optional.of(LS.kv("ssl", LS.bool(url.getProtocol().equals("https"))))
                                        //TODO: Add index
                                )
                        )
                ))
        );

        return LS.config(
                LS.section("input", inputPlugins.toArray(new LS.Plugin[inputPlugins.size()])),
                LS.section("filter", filterPlugins.toArray(new LS.Plugin[filterPlugins.size()])),
                LS.section("output", outputPlugins.toArray(new LS.Plugin[outputPlugins.size()]))
        ).serialize();
    }

    private static URL parseURL(String url) {
        try {
            return new URL(url);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Not valid url: " + url, e);
        }
    }

    private static boolean isRunningInDocker() {
        return System.getenv().containsKey("MESOS_CONTAINER_NAME");
    }

    private static <T> T[] filterEmpties(Class<T> type, Optional<T>... optionals) {
        return Arrays.stream(optionals).filter(Optional::isPresent).map(Optional::get).toArray(size -> (T[]) Array.newInstance(type, size));
    }

    @SafeVarargs
    private static <T> List<T> optionalValuesToList(Optional<T>... optionals) {
        return Arrays.stream(optionals).filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());
    }

    public void run(ExecutorBootConfiguration bootConfiguration) {
        LOGGER.info("Starting the Logstash Process.");

        Process process;
        try {
            String[] command = {
                    LOGSTASH_PATH,
                    "--log", "/var/log/logstash.log",
                    "-e", serialize(bootConfiguration)
            };

            final HashMap<String, String> envs = new HashMap<>();
//            envs.putAll(System.getenv());
            envs.put("PATH", System.getenv("PATH"));
            envs.put("LS_HEAP_SIZE", System.getProperty("mesos.logstash.logstash.heap.size"));
            envs.put("HOME", "/root");

            String[] env = envs.entrySet().stream().map(kv -> kv.getKey() + "=" + kv.getValue()).toArray(String[]::new);
            LOGGER.info("Starting subprocess: " + String.join(" ", env) + " " + String.join(" ", command));
            process = Runtime.getRuntime().exec(command, env);
        } catch (IOException e) {
            throw new RuntimeException("Failed to start Logstash", e);
        }

        try {
            inputStreamForEach((s) -> LOGGER.info("Logstash stdout: " + s), process.getInputStream());
            inputStreamForEach((s) -> LOGGER.warn("Logstash stderr: " + s), process.getErrorStream());

            process.waitFor();

            LOGGER.info("Logstash quit with exit={}", process.exitValue());
            if (process.exitValue() != 0) {
                throw new RuntimeException("Logstash quit with exit=" + process.exitValue());
            }
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
