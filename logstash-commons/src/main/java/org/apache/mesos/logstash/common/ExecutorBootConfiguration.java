package org.apache.mesos.logstash.common;

import java.io.Serializable;
import java.util.Arrays;

public class ExecutorBootConfiguration implements Serializable {
    private String mesosAgentId;
    private String elasticSearchUrl;

    private boolean enableSyslog;
    private int syslogPort;

    private boolean enableCollectd;
    private int collectdPort;

    private boolean enableFile;
    private String[] filePaths;

    public ExecutorBootConfiguration(String mesosAgentId) {
        this.mesosAgentId = mesosAgentId;
    }

    public String getMesosAgentId() {
        return mesosAgentId;
    }

    public void setMesosAgentId(String mesosAgentId) {
        this.mesosAgentId = mesosAgentId;
    }

    public String getElasticSearchUrl() {
        return elasticSearchUrl;
    }

    public void setElasticSearchUrl(String elasticSearchUrl) {
        this.elasticSearchUrl = elasticSearchUrl;
    }

    public boolean isEnableSyslog() {
        return enableSyslog;
    }

    public void setEnableSyslog(boolean enableSyslog) {
        this.enableSyslog = enableSyslog;
    }

    public int getSyslogPort() {
        return syslogPort;
    }

    public void setSyslogPort(int syslogPort) {
        this.syslogPort = syslogPort;
    }

    public boolean isEnableCollectd() {
        return enableCollectd;
    }

    public void setEnableCollectd(boolean enableCollectd) {
        this.enableCollectd = enableCollectd;
    }

    public int getCollectdPort() {
        return collectdPort;
    }

    public void setCollectdPort(int collectdPort) {
        this.collectdPort = collectdPort;
    }

    public boolean isEnableFile() {
        return enableFile;
    }

    public void setEnableFile(boolean enableFile) {
        this.enableFile = enableFile;
    }

    public String[] getFilePaths() {
        return filePaths;
    }

    public void setFilePaths(String[] filePaths) {
        this.filePaths = filePaths;
    }

    @Override
    public String toString() {
        return "ExecutorBootConfiguration{" +
                "mesosAgentId='" + mesosAgentId + '\'' +
                ", elasticSearchUrl='" + elasticSearchUrl + '\'' +
                ", enableSyslog=" + enableSyslog +
                ", syslogPort=" + syslogPort +
                ", enableCollectd=" + enableCollectd +
                ", collectdPort=" + collectdPort +
                ", enableFile=" + enableFile +
                ", filePaths=" + Arrays.toString(filePaths) +
                '}';
    }
}
