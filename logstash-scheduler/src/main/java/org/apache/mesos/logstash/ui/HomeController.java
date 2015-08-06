package org.apache.mesos.logstash.ui;

import org.apache.mesos.logstash.common.LogstashProtos;
import org.apache.mesos.logstash.config.ConfigManager;
import org.apache.mesos.logstash.config.Configuration;
import org.apache.mesos.logstash.config.LogstashSystemProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

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
    private final Configuration settings;

    @Autowired HomeController(ConfigManager configManager, Configuration settings) {
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

    @RequestMapping(method = POST, value = "/api/configs", consumes = "application/json", produces= "application/json")
    @ResponseBody
    public Config createConfig(@RequestBody Config config)
        throws IOException, ExecutionException, InterruptedException {
        configManager.save(
            LogstashProtos.LogstashConfig.newBuilder()
                .setType(DOCKER)
                    // Ensure that the name matches the URL.
                .setConfig(config.getInput())
                .setFrameworkName(config.getName())
                .build());

        return config;
    }

    @RequestMapping(method = RequestMethod.PUT, value = "/api/configs", consumes = "application/json", produces= "application/json")
    @ResponseBody
    public Config updateConfig(@RequestParam("name") String name, @RequestBody Config config)
        throws IOException, ExecutionException, InterruptedException {
        configManager.save(
            LogstashProtos.LogstashConfig.newBuilder()
                .setType(DOCKER)
                    // Ensure that the name matches the URL.
                .setConfig(config.getInput())
                .setFrameworkName(name)
                .build());

        config.setName(name);
        return config;
    }

    @RequestMapping(method = RequestMethod.GET, value = "/api/host-config")
    @ResponseBody
    public Config getHostConfig() throws IOException {
        List<Config> output = getConfigsOfType(HOST);
        return (output.size() > 0) ? output.get(0) : new Config(HOST_FILE_NAME, "");
    }

    @RequestMapping(method = RequestMethod.PUT, value = "/api/host-config")
    @ResponseBody
    public Config updateHostConfig(Config config)
        throws IOException, ExecutionException, InterruptedException {
        configManager.save(
            LogstashProtos.LogstashConfig.newBuilder()
                .setType(HOST)
                .setConfig(config.getInput())
                .setFrameworkName(HOST_FILE_NAME)
                .build());

        return config;
    }

    @RequestMapping(method = RequestMethod.GET, value = "/app/settings")
    @ResponseBody
    public Configuration getSettings() {
        return settings;
    }

    private List<Config> getConfigsOfType(LogstashProtos.LogstashConfig.LogstashConfigType type) {
        return configManager
            .getLatestConfig().stream()
            .filter(c -> c.getType().equals(type))
            .map(Config::fromMapEntry)
            .collect(Collectors.toList());
    }

    @RequestMapping(method = RequestMethod.DELETE, value = "/api/configs")
    @ResponseBody
    public String deleteConfig(@RequestParam("name") String name)
        throws IOException, ExecutionException, InterruptedException {
        configManager.delete(name);
        return "Removed config " + name;
    }
}
