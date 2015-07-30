package org.apache.mesos.logstash.state;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.mesos.MesosNativeLibrary;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.logstash.common.LogstashProtos;
import org.apache.mesos.logstash.common.LogstashProtos.SchedulerMessage;
import org.apache.mesos.logstash.config.LogstashSettings;
import org.apache.mesos.state.State;
import org.apache.mesos.state.Variable;
import org.apache.mesos.state.ZooKeeperState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.mesos.logstash.common.LogstashProtos.SchedulerMessage.SchedulerMessageType.NEW_CONFIG;

/**
 * Manages the persists needs of the scheduler.  It handles frameworkID and the list of tasks on each host.
 */
@Component
public class PersistentState implements IPersistentState {

  private static final String FRAMEWORK_ID_KEY = "frameworkId";
  private static final String LATEST_CONFIG_KEY = "latestConfig";

  private State zkState;

  @Autowired
  public PersistentState(LogstashSettings logstashSettings) {

    if (logstashSettings.getNativeLibrary() != null) {
      MesosNativeLibrary.load(logstashSettings.getNativeLibrary());
    }

    this.zkState = new ZooKeeperState(logstashSettings.getStateZkServers(),
      logstashSettings.getStateZkTimeout(),
      TimeUnit.MILLISECONDS,
      "/logstash-mesos/" + logstashSettings.getFrameworkName());
  }

  @Override public FrameworkID getFrameworkID() throws InterruptedException, ExecutionException, InvalidProtocolBufferException {
    byte[] existingFrameworkId = zkState.fetch(FRAMEWORK_ID_KEY).get().value();
    if (existingFrameworkId.length > 0) {
      return FrameworkID.parseFrom(existingFrameworkId);
    } else {
      return null;
    }
  }

  @Override public void setFrameworkId(FrameworkID frameworkId) throws InterruptedException,
    ExecutionException {
    Variable value = zkState.fetch(FRAMEWORK_ID_KEY).get();
    value = value.mutate(frameworkId.toByteArray());
    zkState.store(value).get();
  }

  @Override public boolean removeFrameworkId() throws ExecutionException, InterruptedException {
    Variable value = zkState.fetch(FRAMEWORK_ID_KEY).get();
    return zkState.expunge(value).get();
  }


  @Override public void setLatestConfig(List<LogstashProtos.LogstashConfig> configs)
      throws ExecutionException, InterruptedException {
    SchedulerMessage message = SchedulerMessage.newBuilder()
        .addAllConfigs(configs)
        .setType(NEW_CONFIG)
        .build();

    Variable value = zkState.fetch(LATEST_CONFIG_KEY).get();
    value = value.mutate(message.toByteArray());
    zkState.store(value).get();
  }


  @Override public SchedulerMessage getLatestConfig()
      throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
    byte[] existingConfig = zkState.fetch(LATEST_CONFIG_KEY).get().value();
    if (existingConfig.length > 0) {
      return SchedulerMessage.parseFrom(existingConfig);
    } else {
      return null;
    }
  }
}