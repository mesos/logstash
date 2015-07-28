package org.apache.mesos.logstash.state;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.mesos.MesosNativeLibrary;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.logstash.config.LogstashSettings;
import org.apache.mesos.state.State;
import org.apache.mesos.state.Variable;
import org.apache.mesos.state.ZooKeeperState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Manages the persists needs of the scheduler.  It handles frameworkID and the list of tasks on each host.
 */
@Component
public class PersistentState {

  private static final String FRAMEWORK_ID_KEY = "frameworkId";

  private State zkState;
  private LogstashSettings logstashSettings;

  @Autowired
  public PersistentState(LogstashSettings logstashSettings) {

    if (logstashSettings.getNativeLibrary() != null) {
      MesosNativeLibrary.load(logstashSettings.getNativeLibrary());
    }

    this.zkState = new ZooKeeperState(logstashSettings.getStateZkServers(),
      logstashSettings.getStateZkTimeout(),
      TimeUnit.MILLISECONDS,
      "/logstash-mesos/" + logstashSettings.getFrameworkName());
    this.logstashSettings = logstashSettings;
  }

  public FrameworkID getFrameworkID() throws InterruptedException, ExecutionException, InvalidProtocolBufferException {
    byte[] existingFrameworkId = zkState.fetch(FRAMEWORK_ID_KEY).get().value();
    if (existingFrameworkId.length > 0) {
      return FrameworkID.parseFrom(existingFrameworkId);
    } else {
      return null;
    }
  }

  public void setFrameworkId(FrameworkID frameworkId) throws InterruptedException,
    ExecutionException {
    Variable value = zkState.fetch(FRAMEWORK_ID_KEY).get();
    value = value.mutate(frameworkId.toByteArray());
    zkState.store(value).get();
  }

  public boolean removeFrameworkId() throws ExecutionException, InterruptedException {
    Variable value = zkState.fetch(FRAMEWORK_ID_KEY).get();
    return zkState.expunge(value).get();
  }

  /**
   * Get serializable object from store.
   *
   * @return serialized object or null if none
   * @throws ExecutionException
   * @throws InterruptedException
   * @throws IOException
   * @throws ClassNotFoundException
   */
  @SuppressWarnings("unchecked")
  private <T extends Object> T get(String key) throws InterruptedException, ExecutionException,
    IOException, ClassNotFoundException {
    byte[] existingNodes = zkState.fetch(key).get().value();
    if (existingNodes.length > 0) {
      ByteArrayInputStream bis = new ByteArrayInputStream(existingNodes);
      ObjectInputStream in = null;
      try {
        in = new ObjectInputStream(bis);
        // generic in java lose their runtime information, there is no way to get this casted without
        // the need for the SuppressWarnings on the method.
        return (T) in.readObject();
      } finally {
        try {
          bis.close();
        } finally {
          if (in != null) {
            in.close();
          }
        }
      }
    } else {
      return null;
    }
  }

  /**
   * Set serializable object in store.
   *
   * @throws ExecutionException
   * @throws InterruptedException
   * @throws IOException
   */
  private <T extends Object> void set(String key, T object) throws InterruptedException,
    ExecutionException, IOException {
    Variable value = zkState.fetch(key).get();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream out = null;
    try {
      out = new ObjectOutputStream(bos);
      out.writeObject(object);
      value = value.mutate(bos.toByteArray());
      zkState.store(value).get();
    } finally {
      try {
        if (out != null) {
          out.close();
        }
      } finally {
        bos.close();
      }
    }
  }
}