package org.apache.mesos.logstash.executor;

import com.github.dockerjava.api.command.EventCallback;
import java.util.*;

/**
 * Created by ero on 15/06/15.
 */
public interface DockerInfo {
    Map<String, LogstashInfo> getContainersThatWantLogging();
    void attachEventListener(EventCallback eventCallback);
    String startContainer(String imageId);
    void stopContainer(String containerId);
    void execInContainer(String containerId, String command);
}
