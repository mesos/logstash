package org.apache.mesos.logstash.common;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;

public class ExecutorBootConfiguration implements Serializable {
    private static final long serialVersionUID = -7053478803581002057L;

    private String mesosAgentId;
    private String[] elasticSearchHosts;
    private String elasticsearchIndex;

    private boolean enableSyslog;
    private int syslogPort;

    private boolean enableCollectd;
    private int collectdPort;

    private boolean enableFile;
    private String[] filePaths;
    private String logstashConfigTemplate;
    private Boolean elasticsearchSSL;
    private String logstashStartConfigTemplate;

    public ExecutorBootConfiguration(String mesosAgentId) {
        this.mesosAgentId = mesosAgentId;
    }

    public String getMesosAgentId() {
        return mesosAgentId;
    }

    public void setMesosAgentId(String mesosAgentId) {
        this.mesosAgentId = mesosAgentId;
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
        return new ToStringBuilder(this)
                .append("mesosAgentId", mesosAgentId)
                .append("elasticSearchHosts", elasticSearchHosts)
                .append("elasticsearchIndex", elasticsearchIndex)
                .append("enableSyslog", enableSyslog)
                .append("syslogPort", syslogPort)
                .append("enableCollectd", enableCollectd)
                .append("collectdPort", collectdPort)
                .append("enableFile", enableFile)
                .append("filePaths", filePaths)
                .append("logstashConfigTemplate", logstashConfigTemplate)
                .toString();
    }

    public String getLogstashConfigTemplate() {
        return logstashConfigTemplate;
    }

    public void setLogstashConfigTemplate(String logstashConfigTemplate) {
        this.logstashConfigTemplate = logstashConfigTemplate;
    }

    public String[] getElasticSearchHosts() {
        return elasticSearchHosts;
    }

    public void setElasticSearchHosts(String ... elasticSearchHosts) {
        this.elasticSearchHosts = elasticSearchHosts;
    }

    public String getElasticsearchIndex() {
        return elasticsearchIndex;
    }

    public void setElasticsearchIndex(String elasticsearchIndex) {
        this.elasticsearchIndex = elasticsearchIndex;
    }

    public Boolean getElasticsearchSSL() {
        return elasticsearchSSL;
    }

    public void setElasticsearchSSL(Boolean elasticsearchSSL) {
        this.elasticsearchSSL = elasticsearchSSL;
    }

    public void setLogstashStartConfigTemplate(String logstashStartConfigTemplate) {
        this.logstashStartConfigTemplate = logstashStartConfigTemplate;
    }

    public String getLogstashStartConfigTemplate() {
        return logstashStartConfigTemplate;
    }
}
