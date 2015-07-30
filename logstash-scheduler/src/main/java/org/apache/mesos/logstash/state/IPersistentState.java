package org.apache.mesos.logstash.state;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.mesos.Protos;
import org.apache.mesos.logstash.common.LogstashProtos;

import java.util.List;
import java.util.concurrent.ExecutionException;


public interface IPersistentState {
    Protos.FrameworkID getFrameworkID() throws InterruptedException, ExecutionException,
        InvalidProtocolBufferException;

    void setFrameworkId(Protos.FrameworkID frameworkId) throws InterruptedException,
      ExecutionException;

    boolean removeFrameworkId() throws ExecutionException, InterruptedException;

    void setLatestConfig(List<LogstashProtos.LogstashConfig> configs)
          throws ExecutionException, InterruptedException;

    LogstashProtos.SchedulerMessage getLatestConfig()
              throws ExecutionException, InterruptedException, InvalidProtocolBufferException;
}
