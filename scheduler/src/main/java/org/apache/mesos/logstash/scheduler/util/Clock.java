package org.apache.mesos.logstash.scheduler.util;

import java.util.Date;

/**
 * Used for mocking time related code.
 */
public class Clock {
    public Date now() {
        return new Date();
    }
}
