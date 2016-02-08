package org.apache.mesos.logstash.executor;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.mesos.logstash.common.ExecutorBootConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Collections.singletonMap;

/**
 * Encapsulates a logstash instance. Keeps track of the current container id for logstash.
 */
public class LogstashService {

    public static final Logger LOGGER = LoggerFactory.getLogger(LogstashService.class);

    public static final String LOGSTASH_PATH = System.getenv("LOGSTASH_PATH");

    static Configuration cfg = new Configuration(Configuration.VERSION_2_3_23);

    static {
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    }

    private static String generateLogstashConfig(ExecutorBootConfiguration bootConfiguration) {
        LOGGER.info("Received " + bootConfiguration.toString());

        try {
            Template template = new Template("logstashConfig", new StringReader(bootConfiguration.getLogstashConfigTemplate()), cfg);
            StringWriter out = new StringWriter();
            Map<String, Object> model = new HashMap<>();
            if (bootConfiguration.isEnableSyslog()) {
                model.put("syslog", map(
                        kv("port", bootConfiguration.getSyslogPort())
                ));
            }
            if (bootConfiguration.isEnableCollectd()) {
                model.put("collectd", map(
                        kv("port", bootConfiguration.getCollectdPort())
                ));
            }
            if (bootConfiguration.isEnableFile()) {
                model.put("file", map(
                        kv("paths", Arrays.stream(bootConfiguration.getFilePaths()).map(path -> (isRunningInDocker() ? "/logstashpaths" : "") + path).toArray(Object[]::new))
                ));
            }
            model.put("mesosAgentId", bootConfiguration.getMesosAgentId());
            if (ArrayUtils.isNotEmpty(bootConfiguration.getElasticSearchHosts())) {
                model.put("elasticsearch", map(
                        kv("hosts", bootConfiguration.getElasticSearchHosts()),
                        kv("ssl", bootConfiguration.getElasticsearchSSL()),
                        kv("index", bootConfiguration.getElasticsearchIndex())
                ));
            }


            template.process(model, out);
            String templateResult = out.toString();
            LOGGER.info("Resulting template: " + templateResult);
            return templateResult;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read template", e);
        } catch (TemplateException e) {
            throw new IllegalArgumentException("Failed to parse template", e);
        }
    }

    private static boolean isRunningInDocker() {
        return System.getenv().containsKey("MESOS_CONTAINER_NAME");
    }

    public void run(ExecutorBootConfiguration bootConfiguration) {
        LOGGER.info("Starting the Logstash Process.");

        Process process;
        try {
            String[] command = {
                    LOGSTASH_PATH, //TOOD: replace with bootConfiguration.getLogstashPath()
                    "--log", "/var/log/logstash.log",
                    "-e", generateLogstashConfig(bootConfiguration)
            };

            final HashMap<String, String> envs = new HashMap<>();
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

    private static class KV<K, V> {
        K key;
        V value;

        public KV(K key, V value) {
            this.key = key;
            this.value = value;
        }

        boolean hasValue() {
            return value != null;
        }
    }

    private static <K, V> KV<K, V> kv(K key, V value) {
        return new KV<>(key, value);
    }

    private static <K, V> Map<K, V> map(KV<K, V>... kvs) {
        return Arrays.stream(kvs).filter(KV::hasValue).collect(Collectors.toMap(kv -> kv.key, kv -> kv.value));
    }
}
