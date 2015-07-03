package org.apache.mesos.logstash.docker;

import com.spotify.docker.client.LogStream;

import java.util.*;

/**
 * Created by ero on 15/06/15.
 */
public interface DockerInfo {
    Set<String> getRunningContainers();
    String getImageNameOfContainer(String containerId);
    String startContainer(String imageId);
    void stopContainer(String containerId);
    LogStream execInContainer(String containerId, String... command);
}
