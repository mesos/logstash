package org.apache.mesos.logstash.common.zookeeper.network;

import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.mesos.logstash.common.network.NetworkUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 */
public class NetworkUtilsTest {

    public static boolean validate(final String ip) {
        return InetAddressValidator.getInstance().isValid(ip);
    }

    private NetworkUtils networkUtils = new NetworkUtils();

    @Test
    // Note: On OSX, when not connected to a network, it will return a IPv6 address, which will not validate properly.
    // Please connect to a network to obtain a IPv4 address.
    public void shouldProvideIPAddress() {
        int port = 1234;
        String string = networkUtils.addressToString(networkUtils.hostSocket(port), true);
        assertTrue(validate(string.replace("http://", "").replace(":" + port, "")));
    }


    @Test
    public void shouldProvideHostname() {
        int port = 1234;
        String string = networkUtils.addressToString(networkUtils.hostSocket(port), false);
        assertFalse(validate(string.replace("http://", "").replace(":" + port, "")));
    }

    @Test
    public void shouldGetDockerIPAddress() throws IOException {
        // Should always be either a valid IP or 127.0.0.1
        assertTrue(validate( networkUtils.getDockerHostIpAddress(networkUtils.getEnvironment())));
    }

    @Test
    public void shouldReturnLocahostOrDocker0AddressWhenNoEnvVar() {
        if ( networkUtils.getDockerHostIpAddress(Collections.emptyMap()).equals(NetworkUtils.LOCALHOST)) {
            assertEquals(NetworkUtils.LOCALHOST, networkUtils.getDockerHostIpAddress(Collections.emptyMap()));
        } else {
            assertEquals(networkUtils.getDocker0AdapterIPAddress(), networkUtils.getDockerHostIpAddress(Collections.emptyMap()));
        }
    }

    @Test
    public void shouldReturnDockerMachineNameWhenIncluded() {
        HashMap<String, String> map = new HashMap<>();
        String dev = "dev";
        map.put(NetworkUtils.DOCKER_MACHINE_NAME, dev);
        assertEquals(dev, networkUtils.getDockerMachineName(map));
    }
}