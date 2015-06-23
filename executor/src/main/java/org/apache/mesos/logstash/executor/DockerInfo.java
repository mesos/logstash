package org.apache.mesos.logstash.executor;


import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.EventCallback;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.Event;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ExecutorService;

/**
 * Created by ero on 15/06/15.
 */
public interface DockerInfo {
    Map<String, LogstashInfo> getContainersThatWantsLogging();
    void attachEventListener(EventCallback eventCallback);
}
