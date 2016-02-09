package org.apache.mesos.logstash.scheduler;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "enable")
public class Features {

    private boolean failover = true;
    private boolean collectd;
    private boolean syslog;
    private boolean file;
    private boolean docker = true;

    //TODO (mwl): Seems unused?
    private Boolean useIpAddress = false;

    public boolean isDocker() {
        return docker;
    }

    public void setDocker(boolean docker) {
        this.docker = docker;
    }

    public boolean isFailover() {
        return failover;
    }

    public void setFailover(boolean failover) {
        this.failover = failover;
    }

    public boolean isCollectd() {
        return collectd;
    }

    public void setCollectd(boolean collectd) {
        this.collectd = collectd;
    }

    public boolean isSyslog() {
        return syslog;
    }

    public void setSyslog(boolean syslog) {
        this.syslog = syslog;
    }

    public boolean isFile() {
        return file;
    }

    public void setFile(boolean file) {
        this.file = file;
    }

    public Boolean getUseIpAddress() {
        return useIpAddress;
    }

}
