package org.apache.mesos.logstash.scheduler.ui;

import org.apache.mesos.logstash.scheduler.ConfigManager;
import org.apache.mesos.logstash.scheduler.ConfigMonitor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.websocket.server.PathParam;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.springframework.web.bind.annotation.RequestMethod.GET;
import static org.springframework.web.bind.annotation.RequestMethod.POST;

@Controller
public class HomeController {

    public static final String HOST_FILE_NAME = "ui";
    private final ConfigMonitor dockerMonitor;
    private final ConfigMonitor hostMonitor;
    private final ConfigManager configManager;

    @Autowired
    HomeController(@Qualifier("docker") ConfigMonitor dockerMonitor,
                   @Qualifier("host") ConfigMonitor hostMonitor,
                   ConfigManager configManager) {
        this.dockerMonitor = dockerMonitor;
        this.hostMonitor = hostMonitor;
        this.configManager = configManager;
    }

    @RequestMapping(method = GET, value = {"/", "/dashboard", "/config", "/nodes"}, produces = {"text/html"})
    public String home() {
        return "index";
    }

    @RequestMapping(method = GET, value = "/api/configs")
    @ResponseBody
    public List<Config> listConfigs() {
        return configManager.getConfig()
                .getDockerConfig().entrySet().stream()
                .map(Config::fromMapEntry)
                .collect(Collectors.toList());
    }

    @RequestMapping(method = POST, value = "/api/configs")
    @ResponseBody
    public void createConfig(Config config) throws IOException {
        // TODO: Validate name and input.
        dockerMonitor.save(config.getName(), config.getInput());
    }

    @RequestMapping(method = RequestMethod.PUT, value = "/api/configs/{name}")
    public String updateConfig(@PathParam("name") String name, Config config) throws IOException {
        // TODO: Validate name and input.
        // Ensure that the name matches the URL.
        dockerMonitor.save(name, config.getInput());

        // TODO: This redirect does not work.
        return "index";
    }

    @RequestMapping(method = RequestMethod.GET, value = "/api/host-config")
    @ResponseBody
    public Config getHostConfig() throws IOException {
        String output = configManager.getConfig().getHostConfig().get(HOST_FILE_NAME);
        return new Config(HOST_FILE_NAME, output);
    }

    @RequestMapping(method = RequestMethod.PUT, value = "/api/host-config")
    @ResponseBody
    public void updateHostConfig(Config config) throws IOException {
        hostMonitor.save(HOST_FILE_NAME, config.getInput());
    }
}
