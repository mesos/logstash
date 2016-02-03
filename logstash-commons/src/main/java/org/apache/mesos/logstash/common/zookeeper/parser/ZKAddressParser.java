package org.apache.mesos.logstash.common.zookeeper.parser;

import org.apache.mesos.logstash.common.zookeeper.exception.ZKAddressException;
import org.apache.mesos.logstash.common.zookeeper.model.ZKAddress;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Ensures ZooKeeper addresses are formatted properly
 *
 * IMPORTANT: Different components in the framework require different ZK address strings.
 *
 * 1) The ZK State requires a ZK servers url
 *
 * host1:port1,host2:port2
 *
 * 2) The MesosSchedulerDriver requires a full ZK url
 *
 * zk://host1:port1,host2:port2/mesos
 *
 * 3) The Elasticsearch ZK plugin requires ZK servers PLUS path WITHOUT the zk:// prefix
 *
 * host1:port1,host2:port2/mesos
 *
 */
public class ZKAddressParser {
    private static final String ZK_PREFIX_REGEX = "^" + ZKAddress.ZK_PREFIX + ".*";

    public List<ZKAddress> validateZkUrl(final String zkUrl) {
        final List<ZKAddress> zkList = new ArrayList<>();

        // Ensure that string is prefixed with "zk://"
        Matcher matcher = Pattern.compile(ZK_PREFIX_REGEX).matcher(zkUrl);
        if (!matcher.matches()) {
            throw new ZKAddressException(zkUrl);
        }

        // Strip zk prefix and spaces
        String zkStripped = zkUrl.replace(ZKAddress.ZK_PREFIX, "").replace(" ", "");

        // Split address by commas
        String[] split = zkStripped.split(",");

        // Validate and add each split
        for (String s : split) {
            zkList.add(new ZKAddress(s));
        }

        // Return list of zk addresses
        return zkList;
    }
}
