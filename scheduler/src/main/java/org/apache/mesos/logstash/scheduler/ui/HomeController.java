package org.apache.mesos.logstash.scheduler.ui;

import org.apache.mesos.logstash.scheduler.ConfigManager;
import org.apache.mesos.logstash.scheduler.ConfigMonitor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.websocket.server.PathParam;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.springframework.web.bind.annotation.RequestMethod.GET;
import static org.springframework.web.bind.annotation.RequestMethod.POST;

@Controller
public class HomeController {

    private final ConfigMonitor configMonitor;
    private final ConfigManager configManager;

    @Autowired
    HomeController(@Qualifier("docker") ConfigMonitor configMonitor, ConfigManager configManager) {
        this.configMonitor = configMonitor;
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
        configMonitor.save(config.getName(), config.getInput());
    }

    @RequestMapping(method = RequestMethod.PUT, value = "/api/configs/{name}")
    public String updateConfig(@PathParam("name") String name, Config config) throws IOException {
        // TODO: Validate name and input.
        // Ensure that the name matches the URL.
        configMonitor.save(name, config.getInput());

        return "redirect:/configs";
    }
}
