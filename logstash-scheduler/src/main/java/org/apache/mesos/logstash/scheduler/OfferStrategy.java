package org.apache.mesos.logstash.scheduler;

import org.apache.commons.lang.math.LongRange;
import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.logstash.config.ExecutorConfig;
import org.apache.mesos.logstash.config.LogstashConfig;
import org.apache.mesos.logstash.state.ClusterState;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.List;
import java.util.Optional;

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

    private List<Integer> neededPorts = asList(5000); // TODO: 25/11/2015 Configurable

    private List<OfferRule> acceptanceRules = asList(
            new OfferRule("Host already running task", this::isHostAlreadyRunningTask),
            new OfferRule("Offer did not have enough CPU resources", this::isNotEnoughCPU),
            new OfferRule("Offer did not have enough RAM resources", this::isNotEnoughRAM),
            new OfferRule("Offer did not have ports available", this::isNotWithNeededPorts)
    );

    public OfferResult evaluate(ClusterState clusterState, Protos.Offer offer) {
        final Optional<OfferRule> decline = acceptanceRules.stream().filter(offerRule -> offerRule.rule.accepts(clusterState, offer)).limit(1).findFirst();
        if (decline.isPresent()) {
            return OfferResult.decline(decline.get().declineReason);
        }

        LOGGER.info("Accepted offer: " + offer.getHostname());
        return OfferResult.accept();
    }

    /**
     * Offer result
     */
    public static class OfferResult {
        final boolean acceptable;
        final Optional<String> reason;

        private OfferResult(boolean acceptable, Optional<String> reason) {
            this.acceptable = acceptable;
            this.reason = reason;
        }

        public static OfferResult accept() {
            return new OfferResult(true, Optional.<String>empty());
        }

        public static OfferResult decline(String reason) {
            return new OfferResult(false, Optional.of(reason));
        }
    }

    private boolean isHostAlreadyRunningTask(ClusterState clusterState, Protos.Offer offer) {
        return clusterState.getTaskList().stream().anyMatch(taskInfo -> taskInfo.getSlaveId().equals(offer.getSlaveId()));
    }

    private boolean hasEnoughOfResourceType(List<Protos.Resource> resources, String resourceName, double minSize) {
        for (Protos.Resource resource : resources) {
            if (resourceName.equals(resource.getName())) {
                return resource.getScalar().getValue() >= minSize;
            }
        }

        return false;
    }

    private boolean isNotEnoughCPU(ClusterState clusterState, Protos.Offer offer) {
        return !hasEnoughOfResourceType(offer.getResourcesList(), "cpus", executorConfig.getCpus());
    }

    private boolean isNotEnoughRAM(ClusterState clusterState, Protos.Offer offer) {
        return !hasEnoughOfResourceType(offer.getResourcesList(), "mem", executorConfig.getHeapSize() + logstashConfig.getHeapSize() + executorConfig.getOverheadMem());
    }

    private boolean isNotWithNeededPorts(ClusterState clusterState, Protos.Offer offer) {
        return !neededPorts.stream()
                .allMatch(
                        port -> offer.getResourcesList().stream()
                                .filter(Protos.Resource::hasRanges) // TODO: 23/11/2015 Check wether this can be removed
                                .anyMatch(resource -> portIsInRanges(port, resource.getRanges()))
                );

    }

    private boolean portIsInRanges(int port, Protos.Value.Ranges ranges) {
        return ranges.getRangeList().stream().anyMatch(range -> new LongRange(range.getBegin(), range.getEnd()).containsLong(port));
    }
    /**
     * Rule and reason container object
     */
    private static class OfferRule {
        String declineReason;
        Rule rule;

        public OfferRule(String declineReason, Rule rule) {
            this.declineReason = declineReason;
            this.rule = rule;
        }
    }

    /**
     * Interface for checking offers
     */
    @FunctionalInterface
    private interface Rule {
        boolean accepts(ClusterState clusterState, Protos.Offer offer);
    }
}
