package org.apache.mesos.logstash.scheduler.ui;


import org.apache.mesos.logstash.scheduler.Scheduler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
public class StatService {

    private final ScheduledExecutorService executorService;
    private final SimpMessagingTemplate template;
    private final Scheduler scheduler;

    @Autowired
    StatService(SimpMessagingTemplate template, Scheduler scheduler) {
        this.template = template;
        this.scheduler = scheduler;
        this.executorService = Executors.newSingleThreadScheduledExecutor();
    }

    @PostConstruct
    public void start() {
        executorService.scheduleAtFixedRate(() -> {
            StatMessage stats = new StatMessage(new SecureRandom().nextInt(), Math.random(), Math.random(), Math.random());
            template.convertAndSend("/topic/stats", stats);
        }, 0, 1, TimeUnit.SECONDS);

        executorService.scheduleAtFixedRate(() -> {
            ArrayList<ExecutorMessage.ExecutorInfo> executors = new ArrayList<>();
            Random random = new Random();

            if (random.nextBoolean()) {
                executors.add(new ExecutorMessage.ExecutorInfo("6127-6213-7812-6312", "1221-1221-64-4345-4"));
            }

            if (random.nextBoolean()) {
                executors.add(new ExecutorMessage.ExecutorInfo("5555-6213-7812-1275", "9183-7221-24-3345-1"));
            }

            if (random.nextBoolean()) {
                executors.add(new ExecutorMessage.ExecutorInfo("1112-3112-8463-1273", "4113-6222-22-3335-3"));
            }

            ExecutorMessage message = new ExecutorMessage(executors);

            template.convertAndSend("/topic/nodes", message);
        }, 0, 3, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void stop() {
        executorService.shutdown();
    }
}
