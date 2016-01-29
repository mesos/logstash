package org.apache.mesos.logstash.scheduler;

import org.apache.commons.lang.math.LongRange;
import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.logstash.config.ExecutorConfig;
import org.apache.mesos.logstash.config.LogstashConfig;
import org.apache.mesos.logstash.state.ClusterState;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;

/**
 * Offer strategy
 */
@Component
public class OfferStrategy {
    private static final Logger LOGGER = Logger.getLogger(OfferStrategy.class);

    @Inject
    private ExecutorConfig executorConfig;

    @Inject
    private LogstashConfig logstashConfig;

    @Inject
    private Features features;

    private List<Rule> acceptanceRules = asList(this::hostRule, this::cpuRule, this::ramRule, this::portsRule);

    private List<Integer> neededPorts() {
        final ArrayList<Integer> ports = new ArrayList<>();
        if (features.isSyslog()) {
            ports.add(logstashConfig.getSyslogPort());
        }
        if (features.isCollectd()) {
            ports.add(logstashConfig.getCollectdPort());
        }
        return ports;
    }

    public OfferResult evaluate(ClusterState clusterState, Protos.Offer offer) {
        final List<String> complaints =
                acceptanceRules
                        .stream()
                        .flatMap(rule -> rule.complaintsFor(clusterState, offer))
                        .collect(Collectors.toList());
        if (!complaints.isEmpty()) {
            return OfferResult.decline(complaints);
        }

        return OfferResult.accept();
    }

    /**
     * Offer result
     */
    public static class OfferResult {
        final List<String> complaints;

        private OfferResult(List<String> complaints) {
            this.complaints = complaints;
        }

        public boolean acceptable() {
            return complaints.isEmpty();
        }

        public static OfferResult accept() {
            return new OfferResult(Collections.emptyList());
        }

        public static OfferResult decline(List<String> complaints) {
            return new OfferResult(complaints);
        }
    }

    private Stream<String> hostRule(ClusterState clusterState, Protos.Offer offer) {
        return clusterState
                .getTaskList()
                .stream()
                .filter(taskInfo -> taskInfo.getSlaveId().equals(offer.getSlaveId())  && taskInfo.getName().equals("logstash.task"))
                .map(taskInfo -> "host " + taskInfo.getSlaveId().getValue() + " is already running task " + taskInfo.getTaskId().getValue());
    }

    private Stream<String> complaintsForResourceType(List<Protos.Resource> resources, String resourceName, double minSize) {
        double totalSize = resources.stream().filter(resource -> resource.getName().equals(resourceName)).collect(Collectors.summingDouble(resource -> resource.getScalar().getValue()));
        if (totalSize < minSize) {
            return Collections.singletonList("required minimum " + minSize + " " + resourceName + " but offer only has " + totalSize + " in total").stream();
        } else {
            return Arrays.asList(new String[]{}).stream();
        }
    }

    private Stream<String> cpuRule(ClusterState clusterState, Protos.Offer offer) {
        return complaintsForResourceType(offer.getResourcesList(), "cpus", executorConfig.getCpus());
    }

    private Stream<String> ramRule(ClusterState clusterState, Protos.Offer offer) {
        return complaintsForResourceType(offer.getResourcesList(), "mem", executorConfig.getHeapSize() + logstashConfig.getHeapSize() + executorConfig.getOverheadMem());
    }

    private Stream<String> portsRule(ClusterState clusterState, Protos.Offer offer) {
        return neededPorts()
                .stream()
                .filter(
                        port -> offer.getResourcesList().stream()
                                .filter(Protos.Resource::hasRanges) // TODO: 23/11/2015 Check wether this can be removed
                                .noneMatch(resource -> portIsInRanges(port, resource.getRanges()))
                )
                .map(port -> "required port " + port + " but was not in offer");
    }

    private boolean portIsInRanges(int port, Protos.Value.Ranges ranges) {
        return ranges.getRangeList().stream().anyMatch(range -> new LongRange(range.getBegin(), range.getEnd()).containsLong(port));
    }

    /**
     * Interface for checking offers
     */
    @FunctionalInterface
    private interface Rule {
        Stream<String> complaintsFor(ClusterState clusterState, Protos.Offer offer);
    }
}
