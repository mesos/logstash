package org.apache.mesos.logstash.state;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.io.IOException;
import java.io.NotSerializableException;
import java.security.InvalidParameterException;

/**
 * Path utilities
 */
@Component
public class StatePath {
    private static final Logger LOGGER = Logger.getLogger(StatePath.class);

    @Inject
    SerializableState zkState;

    /**
     * Creates the zNode if it does not exist. Will create parent directories.
     * @param key the zNode path
     */
    public void mkdir(String key) throws IOException {
        key = key.replace(" ", "");
        if (key.endsWith("/") && !key.equals("/")) {
            throw new InvalidParameterException("Trailing slash not allowed in zookeeper path");
        }
        String[] split = key.split("/");
        StringBuilder builder = new StringBuilder();
        for (String s : split) {
            builder.append(s);
            if (!s.isEmpty() && !exists(builder.toString())) {
                zkState.set(builder.toString(), null);
            }
            builder.append("/");
        }
    }

    public Boolean exists(String key) throws IOException {
        return zkState.get(key) != null;
    }
}
