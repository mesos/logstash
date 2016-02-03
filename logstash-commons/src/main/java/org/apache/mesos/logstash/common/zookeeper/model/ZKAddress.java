package org.apache.mesos.logstash.common.zookeeper.model;

import org.apache.mesos.logstash.common.zookeeper.exception.ZKAddressException;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Model representing ZooKeeper addresses.
 */
public class ZKAddress {
    public static final String ZK_PREFIX = "zk://";
    private static final String USER_AND_PASS_REG = "([^/@:]+):([^/@:]+)";
    private static final String HOST_AND_PORT_REG = "([A-z0-9-.]+)(?::)([0-9]+)";
    private static final String ZK_NODE_REG = "(/.+)";
    private static final String ADDRESS_REGEX = "^(?:"                       +
                                                USER_AND_PASS_REG + "@)?"   +
                                                HOST_AND_PORT_REG           +
                                                "(?:" + ZK_NODE_REG + ")?";
    public static final String VALID_ZK_URL = "zk://host1:port1,user:pass@host2:port2/path,.../path";
    private String user;
    private String password;
    private String address;
    private String port;
    private String zkNode;
    private final Map<Integer, String> matcherMap = new HashMap<>(5);

    /**
     * Represents a single zookeeper address.
     *
     * @param address Must be in the format [user:password@]host[:port] where [] are optional.
     */
    public ZKAddress(String address) {
        Matcher matcher = Pattern.compile(ADDRESS_REGEX).matcher(address);
        if (!matcher.matches()) {
            throw new ZKAddressException(address);
        }
        for (int i = 0; i < matcher.groupCount() + 1; i++) {
            matcherMap.put(i, matcher.group(i));
        }
        setUser(matcherMap.getOrDefault(1, ""));
        setPassword(matcherMap.getOrDefault(2, ""));
        setAddress(matcherMap.getOrDefault(3, ""));
        setPort(matcherMap.getOrDefault(4, ""));
        setZkNode(matcherMap.getOrDefault(5, ""));
    }

    public String getUser() {
        return user;
    }

    void setUser(String user) {
        this.user = "";
        if (user != null) {
            this.user = user;
        }
    }

    public String getPassword() {
        return password;
    }

    void setPassword(String password) {
        this.password = "";
        if (password != null) {
            this.password = password;
        }
    }

    public String getAddress() {
        return address;
    }

    void setAddress(String address) {
        this.address = "";
        if (address != null) {
            this.address = address;
        }
    }

    public String getPort() {
        return port;
    }

    void setPort(String port) {
        this.port = "";
        if (port != null) {
            this.port = port;
        }
    }

    public String getZkNode() {
        return zkNode;
    }

    void setZkNode(String zkNode) {
        this.zkNode = "";
        if (zkNode != null) {
            this.zkNode = zkNode;
        }
    }
}


