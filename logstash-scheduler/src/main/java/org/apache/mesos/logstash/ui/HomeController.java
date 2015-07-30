package org.apache.mesos.logstash.ui;

import org.apache.mesos.logstash.common.LogType;
import org.apache.mesos.logstash.common.LogstashProtos;
import org.apache.mesos.logstash.config.ConfigManager;
import org.apache.mesos.logstash.config.LogstashSettings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.websocket.server.PathParam;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.mesos.logstash.common.LogstashProtos.LogstashConfig.LogstashConfigType.DOCKER;
import static org.apache.mesos.logstash.common.LogstashProtos.LogstashConfig.LogstashConfigType.HOST;
import static org.springframework.web.bind.annotation.RequestMethod.GET;
import static org.springframework.web.bind.annotation.RequestMethod.POST;

@Controller
public class HomeController {

    public static final String HOST_FILE_NAME = "ui";
    private final ConfigManager configManager;
    private final LogstashSettings settings;

    @Autowired HomeController(ConfigManager configManager, LogstashSettings settings) {
        this.configManager = configManager;
        this.settings = settings;
    }

    @RequestMapping(method = GET, value = {"/", "/dashboard", "/config", "/nodes"}, produces = {
        "text/html"})
    public String home() {
        return "index";
    }

    @RequestMapping(method = GET, value = "/api/configs")
    @ResponseBody
    public List<Config> listConfigs() {
        return configManager
            .getLatestConfig().stream()
            .filter(c -> c.getType().equals(DOCKER))
            .map(Config::fromMapEntry)
            .collect(Collectors.toList());
    }

    @RequestMapping(method = POST, value = "/api/configs")
    @ResponseBody
    public void createConfig(Config config)
        throws IOException, ExecutionException, InterruptedException {
        configManager.save(
            LogstashProtos.LogstashConfig.newBuilder()
                .setType(DOCKER)
                    // Ensure that the name matches the URL.
                .setConfig(config.getName())
                .setFrameworkName(config.getName())
                .build());
    }

    @RequestMapping(method = RequestMethod.PUT, value = "/api/configs/{name}")
    public String updateConfig(@PathParam("name") String name, Config config)
        throws IOException, ExecutionException, InterruptedException {
        configManager.save(
            LogstashProtos.LogstashConfig.newBuilder()
                .setType(DOCKER)
                // Ensure that the name matches the URL.
                .setConfig(name)
                .setFrameworkName(config.getName())
                .build());

        // TODO: This redirect does not work.
        return "index";
    }

    @RequestMapping(method = RequestMethod.GET, value = "/api/host-config")
    @ResponseBody
    public Config getHostConfig() throws IOException {
        List<Config> output = getConfigsOfType(HOST);
        return (output.size() > 0) ? output.get(0) : new Config(HOST_FILE_NAME, "");
    }

    @RequestMapping(method = RequestMethod.PUT, value = "/api/host-config")
    @ResponseBody
    public void updateHostConfig(Config config)
        throws IOException, ExecutionException, InterruptedException {
        configManager.save(
            LogstashProtos.LogstashConfig.newBuilder()
                .setType(HOST)
                .setConfig(config.getInput())
                .setFrameworkName(HOST_FILE_NAME)
                .build());
    }

    @RequestMapping(method = RequestMethod.GET, value = "/app/settings")
    public LogstashSettings getSettings() {
        return settings;
    }

    private List<Config> getConfigsOfType(LogstashProtos.LogstashConfig.LogstashConfigType type) {
        return configManager
            .getLatestConfig().stream()
            .filter(c -> c.getType().equals(type))
            .map(Config::fromMapEntry)
            .collect(Collectors.toList());
    }

    @RequestMapping(method = RequestMethod.DELETE, value = "/api/configs/{name}")
    public void deleteConfig(@PathParam("name") String name)
        throws IOException, ExecutionException, InterruptedException {
        configManager.delete(name);
    }
}
