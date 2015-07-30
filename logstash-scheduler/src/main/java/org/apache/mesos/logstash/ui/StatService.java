package org.apache.mesos.logstash.ui;

import org.apache.mesos.logstash.scheduler.LogstashScheduler;
import org.apache.mesos.logstash.state.ILiveState;
import org.apache.mesos.logstash.ui.packets.TaskListPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
public class StatService {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatService.class);

    private final ScheduledExecutorService executorService;
    private final SimpMessagingTemplate template;
    private final ILiveState liveState;
    private final LogstashScheduler scheduler;

    @Autowired StatService(SimpMessagingTemplate template, ILiveState liveState, LogstashScheduler scheduler) {
        this.template = template;
        this.liveState = liveState;
        this.scheduler = scheduler;
        this.executorService = Executors.newSingleThreadScheduledExecutor();
    }

    @PostConstruct
    public void start() {
        executorService.scheduleAtFixedRate(() ->
            template.convertAndSend("/topic/stats", new StatMessageBuilder()
                .setNumNodes(liveState.getTasks().size())
                .setCpus(Math.random())
                .setMem(Math.random())
                .setDisk(Math.random())
                .build())
            , 0, 1, TimeUnit.SECONDS);

        executorService.scheduleAtFixedRate(() -> {
            try {
                template.convertAndSend("/topic/nodes", TaskListPacket.fromTaskList(liveState.getTasks()));
            } catch (Exception e) {
                LOGGER.error("Could not send node packet.", e);
            }
        }, 0, 1, TimeUnit.SECONDS);

        executorService.scheduleAtFixedRate(scheduler::requestExecutorStats, 0, 10, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void stop() {
        executorService.shutdown();
    }
}
