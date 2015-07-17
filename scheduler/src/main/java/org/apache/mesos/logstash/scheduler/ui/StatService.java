package org.apache.mesos.logstash.scheduler.ui;


import org.apache.mesos.logstash.scheduler.ClusterStatus;
import org.apache.mesos.logstash.scheduler.ui.ExecutorMessage.ExecutorData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Component
public class StatService {

    private final ScheduledExecutorService executorService;
    private final SimpMessagingTemplate template;
    private final ClusterStatus status;

    @Autowired
    StatService(SimpMessagingTemplate template, ClusterStatus status) {
        this.template = template;
        this.status = status;
        this.executorService = Executors.newSingleThreadScheduledExecutor();
    }

    @PostConstruct
    public void start() {
        executorService.scheduleAtFixedRate(() ->
            template.convertAndSend("/topic/stats", new StatMessageBuilder()
                    .setNumNodes(status.getExecutors().size())
                    .setCpus(Math.random())
                    .setMem(Math.random())
                    .setDisk(Math.random())
                    .build())
        , 0, 1, TimeUnit.SECONDS);

        executorService.scheduleAtFixedRate(() -> {

            List<ExecutorData> executors = status.getExecutors().stream()
                    .map(ExecutorData::fromExecutor)
                    .collect(Collectors.toList());

            ExecutorMessage message = new ExecutorMessage(executors);

            template.convertAndSend("/topic/nodes", message);
        }, 0, 3, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void stop() {
        executorService.shutdown();
    }
}
