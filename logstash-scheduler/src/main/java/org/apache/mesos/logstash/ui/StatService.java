package org.apache.mesos.logstash.ui;

import org.apache.log4j.Logger;
import org.apache.mesos.logstash.scheduler.LogstashScheduler;
import org.apache.mesos.logstash.state.LiveState;
import org.apache.mesos.logstash.ui.packets.TaskListPacket;
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

    private static final Logger LOGGER = Logger.getLogger(StatService.class.toString());

    private final ScheduledExecutorService executorService;
    private final SimpMessagingTemplate template;
    private final LiveState liveState;
    private final LogstashScheduler scheduler;

    @Autowired StatService(SimpMessagingTemplate template, LiveState liveState, LogstashScheduler scheduler) {
        this.template = template;
        this.liveState = liveState;
        this.scheduler = scheduler;
        this.executorService = Executors.newSingleThreadScheduledExecutor();
    }

    @PostConstruct
    public void start() {
        executorService.scheduleAtFixedRate(() ->
            template.convertAndSend("/topic/stats", new StatMessageBuilder()
                .setNumNodes(liveState.getNonTerminalTasks().size())
                .setCpus(Math.random())
                .setMem(Math.random())
                .setDisk(Math.random())
                .build())
            , 0, 1, TimeUnit.SECONDS);

        executorService.scheduleAtFixedRate(() -> {
            try {
                template.convertAndSend("/topic/nodes", TaskListPacket.fromTaskList(liveState.getNonTerminalTasks()));
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
