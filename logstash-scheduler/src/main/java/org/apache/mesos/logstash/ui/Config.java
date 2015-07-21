package org.apache.mesos.logstash.ui;

import java.util.Map;

// For Request Mapping
@SuppressWarnings("unused")
public class Config {
    private String name;
    private String input;

    public Config() {
    }

    public Config(String name, String input) {

        this.name = name;
        this.input = input;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    public static Config fromMapEntry(Map.Entry<String, String> e) {
        return new Config(e.getKey(), e.getValue());
    }
}
